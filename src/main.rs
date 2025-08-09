// SET GLOBAL @rpl_replica_ack_pos = '{}:{}'
// set @rpl_replica_ack_pos='1:1'
// SET @rpl_semi_sync_replica = 1, @rpl_semi_sync_slave = 1
// @master_binlog_checksum：MySQL在5.6版本之后为binlog引入了checksum机制，从库需要与主库相关参数保持一致。

use std::{path::Path, time::Duration};

use futures_util::StreamExt;
use mysql_async::{
    BinlogStreamRequest, Conn, OptsBuilder, Pool, binlog::events::EventData, prelude::Queryable,
};
use mysql_common::binlog::BinlogFileHeader;
use tokio::{
    fs::{create_dir_all, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};
mod binlog;

// pub const REPLY_MAGIC_NUM_OFFSET: usize = 0;
// pub const K_PACKET_MAGIC_NUM: u8 = 0xef;
// pub const K_PACKET_FLAG_SYNC: u8 = 0x01;
// pub const K_SYNC_HEADER: [u8; 2] = [K_PACKET_MAGIC_NUM, 0];

// const BINLOG_HEADER: [u8; 4] = BinlogFileHeader::VALUE; // "fe bin"

// Hacky way to cut start [0 0 0 ...]
fn strip_prefix(raw: &[u8]) -> &[u8] {
    let pos = raw.iter().position(|&b| b != 0).unwrap_or(raw.len());
    &raw[pos..]
}

async fn pull_binlog_events(
    conn: Conn,
    mut log_file_pos: u64,
    mut log_file_name: String,
    server_id: u32,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let local_binlog_path = "./logs/";
    create_dir_all(local_binlog_path).await?;

    let request = BinlogStreamRequest::new(server_id)
        .with_filename(log_file_name.as_bytes())
        .with_pos(log_file_pos)
        .with_port(8527)
        .with_user("mysqltest".as_bytes())
        .with_hostname("192.168.101.123".as_bytes());

    let mut events_num = 0;
    let mut binlog_stream = conn.get_binlog_stream(request).await?;

    let mut binlog_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(Path::new(&local_binlog_path).join(&log_file_name))
        .await?;
    // if binlog_file.metadata().await.unwrap().len() == 0 {
        // maybe using old file.
    binlog_file.write_all(&BinlogFileHeader::VALUE).await?;
    // }

    while let Some(event) = binlog_stream.next().await {
        let event = event.unwrap();
        println!("event: {:?}", event);
        events_num += 1;

        // assert that event type is known
        let event_type = event.header().event_type().unwrap();

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
                println!("ROTATE_EVENT::raw:: {:?}", event.data());
                let log_file_name_ =
                    String::from_utf8_lossy(strip_prefix(&event.data()[1..])).to_string();
                println!("ROTATE_EVENT::log_file_name:: {:?}", log_file_name_);

                // if log_file_name != log_file_name_ {
                //     binlog_file.flush().await?;
                //     binlog_file.sync_all().await?;

                    // new binlog file
                    binlog_file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(Path::new(&local_binlog_path).join(&log_file_name_))
                        .await?;
                    binlog_file.set_len(0).await?;
                    binlog_file.write_all(&BinlogFileHeader::VALUE).await?;

                    log_file_name = log_file_name_;

                // }
                
            }
            _ => {}
        }

        let (semi_sync, need_ack) = binlog_stream.get_semi_sync_status();
        if semi_sync && need_ack {
            log_file_pos = event.header().log_pos() as u64;
            println!(
                "reply_ack:: log_file_name: {}, log_file_pos: {}",
                log_file_name, log_file_pos
            );
            binlog_stream
                .get_conn_mut()
                .reply_ack(log_file_pos, log_file_name.as_bytes())
                .await
                .unwrap();
        }

        binlog_file.write_all(&event.data()).await?;

        // let mut conn_ = conn.lock().await;
        // conn.reply_ack(log_file_pos, log_file_name.as_bytes());
        // send_ack(&mut conn, log_file_pos, log_file_name.as_bytes()).await;
        // iterate over rows of an event
        if let EventData::RowsEvent(re) = event.read_data()?.unwrap() {
            let tme = binlog_stream.get_tme(re.table_id());
            for row in re.rows(tme.unwrap()) {
                row.unwrap();
            }
        }
    }

    binlog_file.flush().await?;
    binlog_file.sync_all().await?;

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

    assert!(events_num > 0);
    timeout(Duration::from_secs(10), binlog_stream.close())
        .await
        .unwrap()
        .unwrap();
    println!("exit");
    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let init = vec![
        "SET @rpl_semi_sync_replica = 1",
        "SET @rpl_semi_sync_slave = 1",
        "SET @replica_uuid = 'ad501e78-7144-11f0-84cf-0242ac110005'",
        "SET @slave_uuid = 'ad501e78-7144-11f0-84cf-0242ac110005'",
    ];

    // let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(1, 1).unwrap()).with_reset_connection(false);
    let opts = OptsBuilder::default()
        .ip_or_hostname("localhost")
        .user(Some("root"))
        .pass(Some("123456"))
        // .max_allowed_packet(Some(67108864))
        .tcp_port(3312)
        // .pool_opts(pool_opts)
        .init(init);

    let pool = Pool::new(opts);
    let mut conn = pool.get_conn().await?;
    println!("conn: {:?}", conn);

    let log_file_name = "binlog.000001".to_owned();
    let log_file_pos: u64 = 157;
    let server_id: u32 = 5000;
    {
        let strings: Option<String> = conn
            .query_first("select @@rpl_semi_sync_master_enabled")
            .await?;
        println!("rpl_semi_sync_master_enabled={}", strings.clone().unwrap());
        assert_eq!(strings, Some("1".to_owned()));
    }

    // tokio::spawn(async move {
    //     conn.reply_ack(log_file_pos, log_file_name.as_bytes())
    // });
    // conn.reply_ack(log_file_pos, log_file_name.as_bytes()).await?;

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
            "SET @slave_uuid = 'ad501e78-7144-11f0-84cf-0242ac110005'",
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
