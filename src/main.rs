// SET GLOBAL @rpl_replica_ack_pos = '{}:{}'
// set @rpl_replica_ack_pos='1:1'
// SET @rpl_semi_sync_replica = 1, @rpl_semi_sync_slave = 1
// @master_binlog_checksum：MySQL在5.6版本之后为binlog引入了checksum机制，从库需要与主库相关参数保持一致。

use std::{
    fs::{File, OpenOptions, create_dir_all},
    io::Write,
    path::Path,
};

use futures_util::StreamExt;
use mysql_async::{BinlogStreamRequest, Conn, OptsBuilder, Pool};
use mysql_common::binlog::BinlogFileHeader;
use uuid::Uuid;

use crate::query::{master_server_id, master_server_uuid, rpl_semi_sync_master_enabled};
mod query;

const SYSVAR_DEFINED_SEMI_SYNC: &str = "SET @rpl_semi_sync_replica = 1, @rpl_semi_sync_slave = 1";
const DEFAULT_RELAYLOG_PATH: &str = "./relaylog/";
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
fn new_relaylog_file(filename: &str) -> std::result::Result<File, Box<dyn std::error::Error>> {
    create_dir_all(DEFAULT_RELAYLOG_PATH)?;
    let mut relaylog_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(Path::new(&DEFAULT_RELAYLOG_PATH).join(&filename))?;
    relaylog_file.write_all(&BinlogFileHeader::VALUE)?;

    Ok(relaylog_file)
}

async fn pull_binlog_events(
    conn: Conn,
    mut log_file_pos: u64,
    mut log_file_name: String,
    server_id: u32,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let request = BinlogStreamRequest::new(server_id)
        .with_filename(log_file_name.as_bytes())
        .with_pos(log_file_pos)
        .with_port(8527)
        .with_user("mysqltest".as_bytes())
        .with_hostname("192.168.101.123".as_bytes());

    // let mut events_num = 0;
    let mut binlog_stream = conn.get_binlog_stream(request).await?;

    // New file on start, when file exists truncated it.
    let mut relaylog_file = new_relaylog_file(&log_file_name)?;

    while let Some(event) = binlog_stream.next().await {
        let event = event?;
        log_file_pos = event.header().log_pos() as u64;
        // println!("event: {:?}", event);
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
                let log_file_name_ =
                    String::from_utf8_lossy(strip_prefix(&event.data()[1..])).to_string();
                // println!("ROTATE_EVENT::log_file_name:: {:?}", log_file_name_);

                relaylog_file.flush()?;
                relaylog_file.sync_all()?;

                // new binlog file
                relaylog_file = new_relaylog_file(&log_file_name_)?;
                println!("Switch log {} to {}", log_file_name, log_file_name_);
                log_file_name = log_file_name_;
            }
            _ => {}
        }

        let (semi_sync, need_ack) = binlog_stream.get_semi_sync_status();
        if semi_sync && need_ack {
            match binlog_stream
                .get_conn_mut()
                .reply_ack(log_file_pos, log_file_name.as_bytes())
                .await
            {
                Ok(()) => println!("Ack {} {} replied", log_file_name, log_file_pos),
                Err(e) => println!("Ack {} {} reply failed, {e}", log_file_name, log_file_pos),
            }
        }

        // Write event,
        match event.write(mysql_async::binlog::BinlogVersion::Version4, &relaylog_file) {
            Ok(()) => println!("Data {} {} recorded", log_file_name, log_file_pos),
            Err(e) => println!("Event write failed: {e}"),
        }
    }

    relaylog_file.flush()?;
    relaylog_file.sync_all()?;

    // let (tx, _rx) = mpsc::unbounded_channel::<Event>();
    // let mut rx = UnboundedReceiverStream::new(_rx);
    // println!("conn: {:?}", binlog_stream.get_conn_mut());
    // loop {
    //     println!("aaaa");

    //     select! {
    //         Some(ret) = binlog_stream.next() => {
    //             println!("{:?}", ret);
    //             let event = match ret {
    //                 Ok(evt) => evt,
    //                 Err(e) => {
    //                     return Err(Box::new(e));
    //                 }
    //             };

    //             if let Err(e) = tx.send(event) {
    //                 return Err(Box::new(e));
    //             }
    //         },
    //         Some(event) = rx.next() => {
    //             println!("pos: {}", event.header().log_pos());
    //             println!("event: {:?}", event);

    //             // send_semi_sync_ack(&mut conn_, event.header().log_pos() as u64).await?;

    //             // binlog_stream.reply_ack(event.header().log_pos() as u64, "mysql-bin.000007".to_owned()).await;
    //             // conn.query_drop("show databases").await;
    //             let (semi_sync, need_ack) = binlog_stream.get_semi_sync_status();
    //             if semi_sync && need_ack {
    //                 binlog_stream.get_conn_mut().reply_ack(event.header().log_pos() as u64, "mysql-bin.000008".to_string()).await.unwrap();
    //             }
    //         }
    //         // else => {

    //         // }
    //     }
    // }

    // assert!(events_num > 0);
    // timeout(Duration::from_secs(10), binlog_stream.close())
    //     .await
    //     .unwrap()
    //     .unwrap();
    println!("bye.");
    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // 没有指定UUID，那就自己生成一个
    let id = Uuid::new_v4();
    let slave_uuid = id.to_string();
    let init = vec![
        SYSVAR_DEFINED_SEMI_SYNC.to_owned(),
        format!(
            "SET @replica_uuid = '{}', @slave_uuid = '{}'",
            slave_uuid, slave_uuid
        ),
    ];

    let opts = OptsBuilder::default()
        .ip_or_hostname("localhost")
        .user(Some("root"))
        .pass(Some("123456"))
        // .max_allowed_packet(Some(67108864))
        .tcp_port(3312)
        .init(init);

    let pool = Pool::new(opts);
    let mut conn = pool.get_conn().await?;
    print!(
        "Connected to MySQL Server {}:{}, process id {}",
        conn.opts().ip_or_hostname(),
        conn.opts().tcp_port(),
        conn.id()
    );

    let log_file_name = "binlog.000001".to_owned();
    let log_file_pos: u64 = 157;
    let server_id: u32 = 5000;
    {
        let (v1, v2, v3) = conn.server_version();
        println!(", version {}.{}.{}", v1, v2, v3);
    }

    println!(
        "Master:: var:: server_id={}",
        master_server_id(&mut conn).await?
    );
    println!(
        "Master:: var:: server_uuid={}",
        master_server_uuid(&mut conn).await?.unwrap_or_default()
    );
    println!(
        "Master:: var:: rpl_semi_sync_master_enabled={}",
        rpl_semi_sync_master_enabled(&mut conn).await?
    );
    println!(
        "Master:: opt:: max_allowed_packet={:?}",
        conn.opts().max_allowed_packet()
    );
    println!("Master:: opt:: db_name={:?}", conn.opts().db_name());
    println!("Master:: opt:: compression={:?}", conn.opts().compression());
    println!(
        "Master:: opt:: stmt_cache_size={:?}",
        conn.opts().stmt_cache_size()
    );
    println!(
        "Master:: opt:: tcp_keepalive={:?}",
        conn.opts().tcp_keepalive()
    );
    println!("Master:: opt:: tcp_nodelay={:?}", conn.opts().tcp_nodelay());
    println!(
        "Master:: opt:: wait_timeout={:?}",
        conn.opts().wait_timeout()
    );

    pull_binlog_events(conn, log_file_pos, log_file_name, server_id).await?;

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
