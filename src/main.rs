// SET GLOBAL @rpl_replica_ack_pos = '{}:{}'
// set @rpl_replica_ack_pos='1:1'
// SET @rpl_semi_sync_replica = 1, @rpl_semi_sync_slave = 1
// @master_binlog_checksum：MySQL在5.6版本之后为binlog引入了checksum机制，从库需要与主库相关参数保持一致。

use std::time::Duration;

use futures_util::StreamExt;
use mysql_async::{
    BinlogStreamRequest, Conn, OptsBuilder, Pool, binlog::events::EventData, prelude::Queryable,
};
use tokio::time::timeout;

pub const REPLY_MAGIC_NUM_OFFSET: usize = 0;
pub const K_PACKET_MAGIC_NUM: u8 = 0xef;
pub const K_PACKET_FLAG_SYNC: u8 = 0x01;
pub const K_SYNC_HEADER: [u8; 2] = [K_PACKET_MAGIC_NUM, 0];

async fn pull_binlog_events(
    mut conn: Conn,
    mut log_file_pos: u64,
    mut log_file_name: String,
    server_id: u32,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let request = BinlogStreamRequest::new(server_id)
        .with_filename(log_file_name.as_bytes())
        .with_pos(log_file_pos);

    let mut binlog_stream = conn.get_binlog_stream(request).await?;
    let mut events_num = 0;
    while let Some(event) = binlog_stream.next().await {
        let event = event.unwrap();

        events_num += 1;

        log_file_pos = event.header().log_pos() as u64;
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
                log_file_name = String::from_utf8_lossy(&event.data()[1..]).to_string();
            }
            _ => {}
        }

        println!(
            "log_file_name: {}, log_file_pos: {}",
            log_file_name, log_file_pos
        );
        println!("event: {:?}", event);

        // iterate over rows of an event
        if let EventData::RowsEvent(re) = event.read_data()?.unwrap() {
            let tme = binlog_stream.get_tme(re.table_id());
            for row in re.rows(tme.unwrap()) {
                row.unwrap();
            }
        }
    }
    // binlog_stream
    //     .reply_ack(log_file_pos, log_file_name.as_bytes())
    //     .await
    //     .unwrap();

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
    let mut conn = pool.get_conn().await?;
    println!("conn: {:?}", conn);

    let log_file_name = "mysql-bin.000002".to_owned();
    let log_file_pos: u64 = 126;
    let server_id: u32 = 3000;
    {
        let strings: Option<String> = conn
            .query_first("select @@rpl_semi_sync_master_enabled")
            .await?;
        println!("rpl_semi_sync_master_enabled={}", strings.unwrap());
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
