// SET GLOBAL @rpl_replica_ack_pos = '{}:{}'
// set @rpl_replica_ack_pos='1:1'
// SET @rpl_semi_sync_replica = 1, @rpl_semi_sync_slave = 1
// @master_binlog_checksum：MySQL在5.6版本之后为binlog引入了checksum机制，从库需要与主库相关参数保持一致。

use std::{
    fs::{File, OpenOptions, create_dir_all, remove_dir_all},
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::exit,
};

use byteorder::{ByteOrder, LittleEndian};
use futures_util::StreamExt;
use mysql_async::{BinlogStreamRequest, Conn, OptsBuilder, Pool, binlog::BinlogChecksumAlg};
use mysql_common::binlog::BinlogFileHeader;

use crate::{
    prelude::{MyConfig, NAME, prelude},
    query::{
        ConnExt, binlog_gtid_pos, master_gtid_mode, master_server_id, master_server_uuid,
        rpl_semi_sync_master_enabled, show_replicas, sysvar_log_bin,
    },
};
mod prelude;
mod query;

// pub const REPLY_MAGIC_NUM_OFFSET: usize = 0;
// pub const K_PACKET_MAGIC_NUM: u8 = 0xef;
// pub const K_PACKET_FLAG_SYNC: u8 = 0x01;
// pub const K_SYNC_HEADER: [u8; 2] = [K_PACKET_MAGIC_NUM, 0];

// Hacky way to cut start [0 0 0 ...]
fn strip_prefix(raw: &[u8]) -> &[u8] {
    let pos = raw.iter().position(|&b| b != 0).unwrap_or(raw.len());
    &raw[pos..]
}

// 创建一个新的relaylog文件
//   读取index文件，获取最后一行，通过最后一行记录来计算下一个文件后缀
// relay_log_basename: 基础路径
// relay_log: 文件名前缀
// relay_log_index: index文件名
fn next_relaylog_file(
    relay_log_basename: &str,
    relay_log: &str,
    relay_log_index: &str,
    current_relaylog: &mut String,
) -> std::result::Result<File, std::io::Error> {
    // 只读取10个字节的文件后缀名
    let seek_len = -10;
    // ... \n
    // \n
    // seek_len += 2;
    // emmm, only get extension.

    let mut relay_log_index_file = OpenOptions::new()
        .write(true)
        .append(true)
        .read(true)
        .create(true)
        .open(Path::new(relay_log_basename).join(relay_log_index))?;

    let sequence = if let Ok(_size) = relay_log_index_file.seek(SeekFrom::End(seek_len)) {
        // relay-bin.000000001\n
        //           |----10--|
        //           |----9--|
        let mut buf: Vec<u8> = vec![0; 9];
        relay_log_index_file.read_exact(&mut buf).unwrap();
        let n = String::from_utf8_lossy(&buf).to_string();
        match n.parse() {
            Ok(n) => n,
            Err(_) => {
                log::error!("Relay_log_index parse error, {:?} expect is number", n);
                exit(127);
            }
        }
    } else {
        // init=0
        0
    };

    let relaylog_file_path = Path::new(relay_log_basename).join(format!(
        "{}.{}",
        relay_log,
        format!("{:>09}", sequence + 1)
    ));
    let mut relaylog_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&relaylog_file_path)?;
    relaylog_file.write_all(&BinlogFileHeader::VALUE)?;

    // 写index文件
    current_relaylog.clear();
    current_relaylog.push_str(&relaylog_file_path.display().to_string());
    relay_log_index_file.write_all(format!("{}\n", current_relaylog.clone()).as_bytes())?;
    relay_log_index_file.flush()?;

    Ok(relaylog_file)
}

fn next_dump_event(
    dump_file_basename: &PathBuf,
    current_relaylog: &str,
    log_file_name: &str,
    log_file_pos: u64,
    dump_filename: &mut String,
) -> std::result::Result<File, std::io::Error> {
    let dump_file = dump_file_basename.join(format!(
        "{}_{}_{}.dump",
        current_relaylog, log_file_name, log_file_pos
    ));

    dump_filename.clear();
    dump_filename.push_str(&dump_file.display().to_string());

    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&dump_file)
}

async fn pull_binlog_events(
    conn: Conn,
    mut log_file_name: String,
    request: BinlogStreamRequest<'_>,
    config: &MyConfig,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let relay_log_basename = &config.replica.relay_log_basename.clone().unwrap();

    // 每次启动前清理relaylog的目录文件
    if let Err(e) = remove_dir_all(&relay_log_basename) {
        if e.kind() != ErrorKind::NotFound {
            log::error!("Clean relay_log_basename failed: {}", e);
            exit(1);
        }
    }

    if let Err(e) = create_dir_all(&relay_log_basename) {
        log::error!("Create relay_log_basename failed: {}", e);
        exit(1);
    }

    let relaylog = &config.replica.relay_log.clone().unwrap();
    let relaylog_index = &config.replica.relay_log_index.clone().unwrap();
    let mut current_relaylog = relaylog.to_owned();

    // New file on start, when file exists truncated it.
    let mut relaylog_file = next_relaylog_file(
        relay_log_basename,
        relaylog,
        relaylog_index,
        &mut current_relaylog,
    )?;
    let mut log_file_pos;

    let mut binlog_stream = match conn.get_binlog_stream(request).await {
        Ok(s) => s,
        Err(e) => {
            log::error!("Binlog request failed, {}", e);
            exit(1);
        }
    };
    while let Some(event) = binlog_stream.next().await {
        let event = event?;
        log_file_pos = event.header().log_pos() as u64;
        // println!("event: {:?}", event);
        log::debug!("{:?}", event);
        log::debug!("Event checksum: {:?}", event.checksum());
        // events_num += 1;
        // assert that event type is known
        let event_type = event.header().event_type()?;
        match event_type {
            // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html#sect_protocol_replication_event_rotate
            // Binlog Management
            // The first event is either a START_EVENT_V3 or a FORMAT_DESCRIPTION_EVENT while the last event is either a STOP_EVENT or ROTATE_EVENT.
            mysql_async::binlog::EventType::ROTATE_EVENT => {
                // https://dev.mysql.com/doc/dev/mysql-server/latest/classmysql_1_1binlog_1_1event_1_1Rotate__event.html#details
                // The layout of Rotate_event data part is as follows:
                // +-----------------------------------------------------------------------+
                // | common_header | post_header | position of the first event | file name |
                // start with first event, but after is 0
                // 157 00, 00, 00, 00, 00, 00, 00, 62, 69, 6e, 6c, 6f, 67, 2e, 30, 30, 30, 30, 30, 31
                // println!("ROTATE_EVENT::raw:: {:?}", event.data());

                log::debug!("Event data: {:?}", event.data());

                // Position 8bytes
                let mut pos_bytes = [0_u8; 8];
                event.data().read_exact(&mut pos_bytes)?;
                let pos = u64::from_le_bytes(pos_bytes);
                log::debug!("ROTATE_EVENT pos: {}", pos);

                let should_binlog_bytes = &event.data()[8..];
                // mariadb10.3.39: ROTATE_EVENT should_binlog_bytes: [109, 121, 115, 113, 108, 45, 98, 105, 110, 46, 48, 48, 48, 48, 48, 49, 31, 14, 172, 104]
                // Event { ..., data: [4, 0, 0, 0, 0, 0, 0, 0, 109, 121, 115, 113, 108, 45, 98, 105, 110, 46, 48, 48, 48, 48, 48, 49, 31, 14, 172, 104], footer: BinlogEventFooter { checksum_alg: Some(BINLOG_CHECKSUM_ALG_OFF), checksum_enabled: true }, checksum: [0, 0, 0, 0] }
                // mysql8.0.41: ROTATE_EVENT should_binlog_bytes: [109, 121, 115, 113, 108, 45, 98, 105, 110, 46, 48, 48, 48, 48, 48, 57]
                // Event { ..., data: [4, 0, 0, 0, 0, 0, 0, 0, 109, 121, 115, 113, 108, 45, 98, 105, 110, 46, 48, 48, 48, 48, 48, 52], footer: BinlogEventFooter { checksum_alg: Some(BINLOG_CHECKSUM_ALG_OFF), checksum_enabled: true }, checksum: [0, 0, 0, 0] }
                let mut final_binlog_bytes = Vec::new();
                for &byte in should_binlog_bytes {
                    if (32..=126).contains(&byte) {
                        final_binlog_bytes.push(byte);
                    } else {
                        // 遇到非可打印字符，终止提取
                        break;
                    }
                }

                // log::debug!("ROTATE_EVENT calc_checksum: {}", event.calc_checksum(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32));
                log::debug!(
                    "ROTATE_EVENT should_binlog_bytes: {:?}, final_binlog_bytes: {:?}",
                    should_binlog_bytes,
                    final_binlog_bytes
                );
                log_file_name =
                    String::from_utf8_lossy(strip_prefix(&final_binlog_bytes)).to_string();

                // new binlog file
                // 如果上一个文件只有4个字节，那就不用切文件
                if relaylog_file.metadata().unwrap().len() > BinlogFileHeader::VALUE.len() as u64 {
                    relaylog_file.flush()?;
                    relaylog_file.sync_all()?;
                    relaylog_file = next_relaylog_file(
                        relay_log_basename,
                        relaylog,
                        relaylog_index,
                        &mut current_relaylog,
                    )?;
                }

                // Remarks:
                // Mariadb 切换日志可能会收到两次ROTATE_EVENT事件，测试版本 10.3.39
                log::info!("Rotated log to {}", log_file_name);
                // 对比了原生的binlog，不需要将ROTATE_EVENT写入binlog中。
            }
            _ => {}
        }

        if let Some(skip_event) = config.replica.skip_event.clone() {
            match event.header().event_type() {
                Ok(event_type) => {
                    let event_type_ = event_type as u8;
                    if skip_event.contains(&event_type_) {
                        log::info!(
                            "Ignored event {:?} in skip_event={:?}",
                            event_type,
                            skip_event
                        );
                        continue;
                    }
                }
                Err(e) => {
                    log::error!("Ignored event, {}", e);
                    continue;
                }
            }
        }

        let (semi_sync, need_ack) = binlog_stream.get_semi_sync_status();
        if semi_sync {
            log::info!(
                "Slave semi-sync status {}",
                if need_ack { "on" } else { "off" }
            );
            if need_ack {
                match binlog_stream
                    .get_conn_mut()
                    .reply_ack(log_file_pos, log_file_name.as_bytes())
                    .await
                {
                    Ok(()) => log::info!("Ack {} {} replied", log_file_name, log_file_pos),
                    Err(e) => {
                        log::error!("Ack {} {} reply failed, {e}", log_file_name, log_file_pos)
                    }
                }
            }
        }

        // Checksum
        if event.footer().get_checksum_enabled()
            && config.replica.replica_verify_checksum == Some(1)
        {
            if let Some(event_checksum) = event.checksum() {
                let calc_checksum =
                    event.calc_checksum(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32);
                if LittleEndian::read_u32(&event_checksum) == calc_checksum {
                    log::info!("Event checksum {} {} verified", log_file_name, log_file_pos);
                } else {
                    // 考虑写到一个目录，以事件命名
                    // format: {log_file_name}.{log_file_pos}.dump
                    log::info!(
                        "Event checksum {} {} verification failed",
                        log_file_name,
                        log_file_pos
                    );
                    // relay_log_basename/diag/{current_relaylog}_{log_file_name}_{log_file_pos}.dump
                    let mut dump_filename = String::new();
                    let dump_file_basename = Path::new(relay_log_basename).join("diag");
                    create_dir_all(&dump_file_basename)?;
                    match event.write(
                        mysql_async::binlog::BinlogVersion::Version4,
                        &next_dump_event(
                            &dump_file_basename,
                            &current_relaylog,
                            &log_file_name,
                            log_file_pos,
                            &mut dump_filename,
                        )?,
                    ) {
                        Ok(()) => log::info!(
                            "Event {} {} dumped to {}",
                            &log_file_name,
                            log_file_pos,
                            dump_filename
                        ),
                        Err(e) => log::error!("Event dump failed: {e}"),
                    }
                }
            }
        }

        // Write event,
        match event.write(mysql_async::binlog::BinlogVersion::Version4, &relaylog_file) {
            Ok(()) => log::info!("Event {} {} recorded", log_file_name, log_file_pos),
            Err(e) => log::error!("Event record failed: {e}"),
        }

        // Exit only event catch up.
        let bye_file = Path::new(&config.replica.datadir).join("bye");
        if let Ok(_f) = File::open(&bye_file) {
            log::info!("The bye file {} exists", bye_file.display());
            break;
        }
    }

    relaylog_file.flush()?;
    relaylog_file.sync_all()?;

    println!("bye.");
    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let (config, args, log_file_name, log_file_pos) = prelude()?;

    let log4rs_config_file = option_env!("MYSQL_AVATAR_LOG4RS_FILE").unwrap_or("./log4rs.toml");
    log4rs::init_file(log4rs_config_file, Default::default())?;

    let host = match args.host.clone() {
        Some(host) => host,
        None => config.mysql.host.clone(),
    };

    let user = match args.user.clone() {
        Some(user) => user,
        None => config.mysql.user.clone().unwrap_or(String::from("root")),
    };

    let pass = match args.password.clone() {
        Some(pass) => pass,
        None => config.mysql.pass.clone().unwrap_or_default(),
    };

    let port = match args.port.clone() {
        Some(port) => port,
        None => config.mysql.port.unwrap_or(3306),
    };

    let opts = OptsBuilder::default()
        .ip_or_hostname(host.clone())
        .user(Some(user))
        .pass(Some(pass))
        .max_allowed_packet(config.mysql.max_allowed_packet)
        .tcp_port(port)
        .init(config.mysql.init_sql.clone().unwrap());

    let pool = Pool::new(opts);
    let mut conn = match pool.get_conn().await {
        Ok(c) => c,
        Err(e) => {
            log::error!("Unable to connect to {}:{}, {}", host.clone(), port, e);
            exit(1);
        }
    };
    log::debug!("{:?}", conn);

    let (v1, v2, v3) = conn.server_version();
    let is_maria = conn.is_mariadb().await?;
    log::info!(
        "Connected to {} Server {}:{}, process id {}, version {}.{}.{}",
        if is_maria { "MariaDB" } else { "MySQL" },
        conn.opts().ip_or_hostname(),
        conn.opts().tcp_port(),
        conn.id(),
        v1,
        v2,
        v3
    );

    let master_server_id = master_server_id(&mut conn).await?;
    log::info!("Server_id={}", master_server_id);

    // if args.with_gtid {
    //     log::info!("Binlog_gtid_pos: {:?}", args.filename_or_gtid);
    // } else if args.with_file {

    // } else {
    //     // Unknown
    // }

    let report_host = config
        .replica
        .report_host
        .clone()
        .unwrap_or(format!("{}-avatar", NAME));

    let report_port = config.replica.report_port.unwrap_or(8527);

    let replica_id = match config.replica.replica_id {
        Some(v) => v,
        None => {
            let mut server_ids = Vec::new();
            server_ids.push(master_server_id);
            let mut id = 0;
            for slave in show_replicas(&mut conn).await? {
                if slave.host() == Some(report_host.clone()) && slave.port() == report_port {
                    id = slave.server_id();
                    break;
                }
                server_ids.push(slave.server_id());
            }
            let max = server_ids.iter().max().unwrap_or(&master_server_id);
            // 需要考虑重启，不然会出现很多线程连接上去
            let id = if id > 0 { id } else { *max + 1 };
            log::info!("Automatically set Replica_id={}", id);
            id
        }
    };

    let master_server_uuid = match master_server_uuid(&mut conn).await {
        Ok(v) => v.unwrap_or_default(),
        Err(e) => {
            log::warn!("{e}");
            String::new()
        }
    };

    log::info!("Server_uuid={:?}", master_server_uuid);

    match sysvar_log_bin(&mut conn).await {
        Ok(log_bin) => {
            if log_bin == 1 {
                log::info!("Server log_bin={}", log_bin);
            } else {
                log::error!("Server log_bin={}, expected log_bin=1", log_bin);
                exit(1);
            }
        }
        Err(e) => {
            log::error!("{e}");
        }
    };

    let rpl_semi_sync_master_enabled = match rpl_semi_sync_master_enabled(&mut conn).await {
        Ok(v) => v,
        Err(e) => {
            log::warn!("{e}");
            0
        }
    };

    let rpl_semi_sync_replica_enabled = config
        .replica
        .rpl_semi_sync_replica_enabled
        .unwrap_or_default();
    if rpl_semi_sync_replica_enabled == 1 && rpl_semi_sync_master_enabled != 1 {
        log::error!(
            "Rpl_semi_sync_master_enabled={}, expected rpl_semi_sync_master_enabled=1",
            rpl_semi_sync_master_enabled
        );
        exit(1);
    } else {
        log::info!(
            "Rpl_semi_sync_master_enabled={}",
            rpl_semi_sync_master_enabled
        );
    }

    let gtid_mode = match master_gtid_mode(&mut conn).await {
        Ok(v) => {
            if v {
                "ON"
            } else {
                "OFF"
            }
        }
        Err(e) => {
            log::warn!("{e}");
            "OFF"
        }
    };

    if gtid_mode != "ON" {
        log::warn!("Gtid_mode={:?}, expected open GTID mode", gtid_mode);
    } else {
        log::info!("Gtid_mode={:?}", gtid_mode);
    }

    let gtid = binlog_gtid_pos(&mut conn, log_file_pos, log_file_name.as_bytes()).await?;
    log::info!(
        "Binlog_gtid_pos={:?}, filename={:?}, position={}",
        match gtid {
            Some(v) => v,
            None => String::new(),
        },
        log_file_name.clone(),
        log_file_pos
    );

    match conn.opts().max_allowed_packet() {
        Some(max_allowed_packet) => {
            log::info!("Max_allowed_packet={}", max_allowed_packet)
        }
        None => {
            log::info!("Max_allowed_packet=<Not set>")
        }
    }

    let cloned_log_file_name = log_file_name.clone();

    let request = BinlogStreamRequest::new(replica_id)
        .with_filename(cloned_log_file_name.as_bytes())
        .with_pos(log_file_pos)
        .with_port(report_port)
        .with_user(NAME.as_bytes())
        .with_hostname(report_host.as_bytes());

    pull_binlog_events(conn, log_file_name, request, &config).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::{Read, Seek, SeekFrom},
        path::Path,
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    // #[test]
    // fn test_rotate_event_checksum(){

    //     let final_binlog_bytes= [4, 0, 0, 0, 0, 0, 0, 0, 109, 121, 115, 113, 108, 45, 98, 105, 110, 46, 48, 48, 48, 48, 48, 49];
    //     let recv_bytes = [4, 0, 0, 0, 0, 0, 0, 0, 109, 121, 115, 113, 108, 45, 98, 105, 110, 46, 48, 48, 48, 48, 48, 49, 31, 14, 172, 104];
    //     let checksum = [31, 14, 172, 104];

    //     let v1 = LittleEndian::read_u32(&checksum);
    //     let mut hasher = crc32fast::Hasher::new();

    //     let v2 = LittleEndian::read_u32(&final_binlog_bytes);
    //     assert_eq!(v1, v2);
    // }

    #[test]
    fn test_read_relaylog_index() {
        let mut file = File::open("./data/relay-bin/relay-bin.index").unwrap();
        file.seek(SeekFrom::End(-10)).unwrap();
        let mut buf = String::new();
        file.read_to_string(&mut buf).unwrap();
        println!("data: {buf}");
        let path = Path::new(&buf);
        let sequence: u32 = path
            .extension()
            .unwrap()
            .to_string_lossy()
            .trim_end_matches("\n")
            .parse()
            .unwrap();
        println!("sequence: {:?}", sequence);
        println!(
            "extension: {:?}",
            path.extension()
                .unwrap()
                .to_string_lossy()
                .trim_end_matches("\n")
        );
        let path = Path::new(&buf).with_extension(format!("{:>09}", sequence + 1));
        println!("path: {:?}", path);
    }

    // 模拟一个mysqlserver，给slave发送binlog日志
    #[tokio::test]
    async fn test_mysql_server() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:8080").await?;

        loop {
            let (mut socket, _) = listener.accept().await?;

            tokio::spawn(async move {
                let mut buf = [0; 1024];

                // In a loop, read data from the socket and write the data back.
                loop {
                    let n = match socket.read(&mut buf).await {
                        // socket closed
                        Ok(0) => return,
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("failed to read from socket; err = {:?}", e);
                            return;
                        }
                    };

                    println!("data: {:?}", buf);

                    // Write the data back
                    if let Err(e) = socket.write_all(&buf[0..n]).await {
                        eprintln!("failed to write to socket; err = {:?}", e);
                        return;
                    }
                }
            });
        }
    }
}
