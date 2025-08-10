// SET GLOBAL @rpl_replica_ack_pos = '{}:{}'
// set @rpl_replica_ack_pos='1:1'
// SET @rpl_semi_sync_replica = 1, @rpl_semi_sync_slave = 1
// @master_binlog_checksum：MySQL在5.6版本之后为binlog引入了checksum机制，从库需要与主库相关参数保持一致。

use std::{
    fs::{File, OpenOptions, create_dir_all},
    io::{Read, Write},
    path::Path,
    process::exit,
};

use byteorder::{ByteOrder, LittleEndian};
use clap::Parser;
use futures_util::StreamExt;
use mysql_async::{BinlogStreamRequest, Conn, OptsBuilder, Pool, binlog::BinlogChecksumAlg};
use mysql_common::binlog::BinlogFileHeader;
use uuid::Uuid;

use crate::{
    prelude::{Args, MyConfig, NAME},
    query::{
        binlog_gtid_pos, is_mariadb, master_gtid_mode, master_server_id, master_server_uuid,
        rpl_semi_sync_master_enabled, show_replicas,
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

fn gen_relaylog_filename(relay_log_prefix: &str, filename: &str) -> String {
    format!("{}_{}", relay_log_prefix, filename)
}

// 创建一个新的relaylog文件
fn new_relaylog_file(
    relay_log_prefix: &str,
    filename: &str,
) -> std::result::Result<File, Box<dyn std::error::Error>> {
    let mut relaylog_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(gen_relaylog_filename(relay_log_prefix, filename))?;
    relaylog_file.write_all(&BinlogFileHeader::VALUE)?;

    Ok(relaylog_file)
}

async fn pull_binlog_events(
    conn: Conn,
    mut log_file_name: String,
    request: BinlogStreamRequest<'_>,
    config: &MyConfig,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    // let mut events_num = 0;
    let mut binlog_stream = conn.get_binlog_stream(request).await?;
    let relay_log_basename = config.replica.relay_log_basename.clone().unwrap();
    create_dir_all(&relay_log_basename)?;

    let relay_log_prefix =
        Path::new(&relay_log_basename).join(&config.replica.relay_log.clone().unwrap());
    let relay_log_prefix = relay_log_prefix.to_string_lossy();

    // New file on start, when file exists truncated it.
    let mut relaylog_file = new_relaylog_file(&relay_log_basename, &log_file_name)?;
    let relaylog_index_file =
        Path::new(&relay_log_basename).join(&config.replica.relay_log_index.clone().unwrap());
    // let relaylog_index_file = relaylog_index_file.to_string_lossy();

    let mut relaylog_index_value = gen_relaylog_filename(&relay_log_prefix, &log_file_name);

    let mut log_file_pos;

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
                let data = &event.data()[1..];
                let len = if event.checksum().is_none() {
                    // event.data 最后4位校验码
                    data.len() - 4
                } else {
                    data.len()
                };

                log_file_name = String::from_utf8_lossy(strip_prefix(&data[0..len])).to_string();

                relaylog_file.flush()?;
                relaylog_file.sync_all()?;

                // new binlog file
                relaylog_file = new_relaylog_file(&relay_log_prefix, &log_file_name)?;

                let newfile = gen_relaylog_filename(&relay_log_prefix, &log_file_name);
                if relaylog_index_value != newfile {
                    // 写 index 文件
                    let mut index_file = OpenOptions::new()
                        .write(true)
                        .append(true)
                        .create(true)
                        .open(&relaylog_index_file)?;
                    index_file.write_all(format!("{}\n", newfile).as_bytes())?;
                    index_file.flush()?;
                    relaylog_index_value = newfile;
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
                    let dump_file = format!("{}.{}.dump", log_file_name, log_file_pos);
                    let relaylog_file_dump =
                        new_relaylog_file(&config.replica.relay_log.clone().unwrap(), &dump_file)?;
                    match event.write(
                        mysql_async::binlog::BinlogVersion::Version4,
                        &relaylog_file_dump,
                    ) {
                        Ok(()) => log::info!("Event {} {} dumped", dump_file, log_file_pos),
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
    let args = Args::parse();

    log4rs::init_file("./log4rs.toml", Default::default())?;

    let config_path = "./config.toml";
    let mut config_file = match File::open(config_path) {
        Ok(f) => f,
        Err(e) => {
            println!("Config file {} open failed, {e}", config_path);
            exit(1);
        }
    };

    let mut buf = String::new();
    config_file.read_to_string(&mut buf)?;

    let mut config: MyConfig = toml::from_str(&buf)?;

    let log_file_name = match args.start_filename {
        Some(v) => v,
        None => match config.mysql.start_filename.clone() {
            Some(v) if !v.is_empty() => v,
            _ => {
                println!("Using --with-file, --start-filename is requred");
                exit(1);
            }
        },
    };

    log::info!("Start_filename={}", log_file_name);
    let log_file_pos = match args.start_position {
        Some(v) => v,
        None => match config.mysql.start_position {
            Some(v) if v > 0 => v,
            _ => {
                println!("Using --with-file, --start-position is requred");
                exit(1);
            }
        },
    };
    log::info!("Start_position={}", log_file_pos);

    // let build_time = match option_env!("BUILD_TIME") {
    //     Some(k) => format!(" on {}", k),
    //     None => String::new(),
    // };

    // mysql avatar
    let replica_uuid = match config.replica.replica_uuid.clone() {
        Some(id) => id,
        None => {
            // 没有指定UUID，那就自己生成一个
            Uuid::new_v4().to_string()
        }
    };

    // 是否启动半同步复制
    let rpl_semi_sync_replica_enabled = config
        .replica
        .rpl_semi_sync_replica_enabled
        .unwrap_or_default();

    if config.replica.relay_log_basename.is_none() {
        config.replica.relay_log_basename = Some(
            Path::new(&config.replica.datadir)
                .join("relay-bin")
                .to_string_lossy()
                .to_string(),
        )
    }

    log::info!(
        "Relay_log_basename={}",
        config.replica.relay_log_basename.clone().unwrap()
    );

    if config.replica.relay_log.clone().is_none() {
        config.replica.relay_log = Some(String::from("relay-bin"))
    }

    log::info!("Relay_log={}", config.replica.relay_log.clone().unwrap());
    if config.replica.relay_log_index.clone().is_none() {
        config.replica.relay_log_index = Some(String::from("relay-bin.index"))
    }
    log::info!(
        "Relay_log_index={}",
        config.replica.relay_log_index.clone().unwrap()
    );

    // Replica_verify_checksum
    log::info!(
        "Replica_verify_checksum={}",
        match config.replica.replica_verify_checksum {
            Some(v) => v,
            None => 0,
        }
    );

    let mut init = vec![
        format!(
            "SET @rpl_semi_sync_replica = {}, @rpl_semi_sync_slave = {}",
            rpl_semi_sync_replica_enabled, rpl_semi_sync_replica_enabled
        ),
        format!(
            "SET @replica_uuid = '{}', @slave_uuid = '{}'",
            replica_uuid, replica_uuid
        ),
    ];

    // 可以指定初始化sql
    if let Some(mut init_sqls) = config.mysql.init_sql.clone() {
        init.append(&mut init_sqls);
    }

    let opts = OptsBuilder::default()
        .ip_or_hostname(config.mysql.host.clone())
        .user(config.mysql.user.clone())
        .pass(config.mysql.pass.clone())
        .max_allowed_packet(config.mysql.max_allowed_packet)
        .tcp_port(config.mysql.port.unwrap_or(3306))
        .init(init);

    let pool = Pool::new(opts);
    let mut conn = pool.get_conn().await?;
    log::debug!("{:?}", conn);
    let (v1, v2, v3) = conn.server_version();
    let is_maria = is_mariadb(&mut conn).await?;
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

    let gtid = binlog_gtid_pos(&mut conn, log_file_pos, log_file_name.as_bytes()).await?;
    log::info!(
        "Binlog_gtid_pos: {:?}",
        match gtid {
            Some(v) => v,
            None => String::new(),
        }
    );
    // } else {
    //     // Unknown
    // }

    let report_host = config
        .replica
        .report_host
        .clone()
        .unwrap_or(format!("{}-avatar", NAME));

    let report_port = config.replica.report_port.unwrap_or(8527);

    let slave_id = match config.replica.replica_id {
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
            if id > 0 { id } else { *max + 1 }
        }
    };

    log::info!("Slave_id={}", slave_id);
    let master_server_uuid = match master_server_uuid(&mut conn).await {
        Ok(v) => v.unwrap_or_default(),
        Err(e) => {
            log::warn!("{e}");
            String::new()
        }
    };

    log::info!("Server_uuid={:?}", master_server_uuid);
    log::info!("Replica_uuid={:?}", replica_uuid);

    let rpl_semi_sync_master_enabled = match rpl_semi_sync_master_enabled(&mut conn).await {
        Ok(v) => v,
        Err(e) => {
            log::warn!("{e}");
            0
        }
    };
    log::info!(
        "Rpl_semi_sync_master_enabled={}",
        rpl_semi_sync_master_enabled
    );
    log::info!(
        "Rpl_semi_sync_replica_enabled={}",
        rpl_semi_sync_replica_enabled
    );

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
    log::info!("Gtid_mode={}", gtid_mode);

    match conn.opts().max_allowed_packet() {
        Some(max_allowed_packet) => {
            log::info!("Max_allowed_packet={}", max_allowed_packet)
        }
        None => {
            log::info!("Max_allowed_packet=<default>")
        }
    }

    let cloned_log_file_name = log_file_name.clone();

    let request = BinlogStreamRequest::new(slave_id)
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
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

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
