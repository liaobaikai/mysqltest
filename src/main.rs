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

use futures_util::StreamExt;
use mysql_async::{BinlogStreamRequest, Conn, OptsBuilder, Pool};
use mysql_common::binlog::BinlogFileHeader;
use uuid::Uuid;

use crate::{
    prelude::{MyConfig, NAME},
    query::{
        is_mariadb, master_gtid_mode, master_server_id, master_server_uuid,
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

// 创建一个新的relaylog文件
fn new_relaylog_file(
    datadir: &str,
    filename: &str,
) -> std::result::Result<File, Box<dyn std::error::Error>> {
    create_dir_all(datadir)?;
    let mut relaylog_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(Path::new(datadir).join(&filename))?;
    relaylog_file.write_all(&BinlogFileHeader::VALUE)?;

    Ok(relaylog_file)
}

async fn pull_binlog_events(
    conn: Conn,
    mut log_file_name: String,
    request: BinlogStreamRequest<'_>,
    datadir: String,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    // let mut events_num = 0;
    let mut binlog_stream = conn.get_binlog_stream(request).await?;

    // New file on start, when file exists truncated it.
    let mut relaylog_file = new_relaylog_file(&datadir, &log_file_name)?;
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
                relaylog_file = new_relaylog_file(&datadir, &log_file_name)?;

                // Remarks:
                // Mariadb 切换日志可能会收到两次ROTATE_EVENT事件，测试版本 10.3.39
                log::info!("Rotated log to {}", log_file_name);
                // 对比了原生的binlog，不需要将ROTATE_EVENT写入binlog中。
                continue;
            }
            _ => {}
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

        // Write event,
        match event.write(mysql_async::binlog::BinlogVersion::Version4, &relaylog_file) {
            Ok(()) => log::info!("Event {} {} recorded", log_file_name, log_file_pos),
            Err(e) => log::error!("Event record failed: {e}"),
        }

        // Exit only event catch up.
        if let Ok(_f) = File::open(Path::new(&datadir).join("bye")) {
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

    let config: MyConfig = toml::from_str(&buf)?;

    // let build_time = match option_env!("BUILD_TIME") {
    //     Some(k) => format!(" on {}", k),
    //     None => String::new(),
    // };

    // mysql avatar
    let slave_uuid = match config.local.slave_uuid {
        Some(id) => id,
        None => {
            // 没有指定UUID，那就自己生成一个
            Uuid::new_v4().to_string()
        }
    };

    // 是否启动半同步复制
    let rpl_semi_sync_slave_enabled = config.local.rpl_semi_sync_slave_enabled.unwrap_or_default();
    let mut init = vec![
        format!(
            "SET @rpl_semi_sync_replica = {}, @rpl_semi_sync_slave = {}",
            rpl_semi_sync_slave_enabled, rpl_semi_sync_slave_enabled
        ),
        format!(
            "SET @replica_uuid = '{}', @slave_uuid = '{}'",
            slave_uuid, slave_uuid
        ),
    ];

    // 可以指定初始化sql
    if let Some(mut init_sqls) = config.master.init_sql {
        init.append(&mut init_sqls);
    }

    let opts = OptsBuilder::default()
        .ip_or_hostname(config.master.host)
        .user(config.master.user)
        .pass(config.master.pass)
        .max_allowed_packet(config.master.max_allowed_packet)
        .tcp_port(config.master.port.unwrap_or(3306))
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

    let log_file_name = "mysql-bin.000001".to_owned();
    let log_file_pos: u64 = 4;

    let reg_host = config
        .local
        .register_slave_host
        .unwrap_or(format!("{}-avatar", NAME));

    let reg_port = config.local.register_slave_port.unwrap_or(8527);

    let slave_id = match config.local.slave_id {
        Some(v) => v,
        None => {
            let mut server_ids = Vec::new();
            server_ids.push(master_server_id);
            let mut id = 0;
            for slave in show_replicas(&mut conn).await? {
                if slave.host() == Some(reg_host.clone()) && slave.port() == reg_port {
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

    log::info!("Server_uuid={}", master_server_uuid);
    log::info!("Slave_uuid={}", slave_uuid);

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
        "Rpl_semi_sync_slave_enabled={}",
        rpl_semi_sync_slave_enabled
    );

    let gtid_mode = match master_gtid_mode(&mut conn).await {
        Ok(v) => v,
        Err(e) => {
            log::warn!("{e}");
            false
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
        .with_port(reg_port)
        .with_user(NAME.as_bytes())
        .with_hostname(reg_host.as_bytes());

    pull_binlog_events(conn, log_file_name, request, config.local.datadir).await?;

    Ok(())
}

// mysql-8.0.43/plugin/semisync/semisync_replica_plugin.cc:: repl_semi_slave_request_dump
// mariadb-10.3.39/sql/semisync_slave.cc:: request_transmit

#[cfg(test)]
mod tests {
    use std::io::Write;

    use futures_util::StreamExt;
    use mysql_async::{BinlogStreamRequest, OptsBuilder, Pool};
    use tokio::select;

    #[tokio::test]
    async fn test() {
        let init = vec![
            "SET @rpl_semi_sync_replica = 1",
            "SET @rpl_semi_sync_slave = 1",
            // "SET @server_id=3000",
            "SET @replica_uuid = 'ad501e78-7144-11f0-84cf-0242ac110005'",
        ];

        let opts = OptsBuilder::default()
            .ip_or_hostname("localhost")
            .user(Some("root"))
            .pass(Some("baikai#1234"))
            // .max_allowed_packet(Some(67108864))
            .tcp_port(3306)
            .init(init);

        let pool = Pool::new(opts);
        let conn = pool.get_conn().await.unwrap();
        let request = BinlogStreamRequest::new(4000)
            .with_filename(b"mysql-bin.000007")
            .with_pos(4);

        let mut stream = conn.get_binlog_stream(request).await.unwrap();

        loop {
            select! {
                Some(event) = stream.next() => {
                    println!("{:?}", event);
                },
                // else => {

                // }
            }
        }
    }

    #[test]
    fn test2() {
        let position: u64 = 2646;
        let filename: String = "binlog.000001".to_owned();

        let mut buf = Vec::new();
        buf.write_all(&[0xef]).unwrap();
        buf.write_all(&position.to_le_bytes()[..8]).unwrap();
        buf.write_all(&filename.as_bytes()).unwrap();
        buf.write_all(&[0x00]).unwrap();

        println!("buf: {:02x?}", buf);
    }
}
