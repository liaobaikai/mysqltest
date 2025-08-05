// SET GLOBAL @rpl_replica_ack_pos = '{}:{}'
// set @rpl_replica_ack_pos='1:1'
// SET @rpl_semi_sync_replica = 1, @rpl_semi_sync_slave = 1
// @master_binlog_checksum：MySQL在5.6版本之后为binlog引入了checksum机制，从库需要与主库相关参数保持一致。

use std::time::Duration;

use futures_util::StreamExt;
use mysql_async::{BinlogStreamRequest, OptsBuilder, Pool, prelude::*};
use mysql_common::binlog::events::EventData;
use tokio::time::timeout;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let opts = OptsBuilder::default()
        .ip_or_hostname("localhost")
        .user(Some("root"))
        .pass(Some("123456"))
        .tcp_port(3312)
        .init(vec!["SET @rpl_semi_sync_replica = 1, @rpl_semi_sync_slave = 1"]);

    let pool = Pool::new(opts);
    let conn = pool.get_conn().await?;
    println!("conn: {:?}", conn);

    let request = BinlogStreamRequest::new(3000)
        .with_filename(b"binlog.000002")
        .with_pos(157);
    let mut binlog_stream = conn.get_binlog_stream(request).await?;

    let mut events_num = 0;
    while let Some(event) = binlog_stream.next().await {

        let event = event.unwrap();
        events_num += 1;

        // assert that event type is known
        event.header().event_type().unwrap();

        println!("event: {:?}", event);

        // iterate over rows of an event
        if let EventData::RowsEvent(re) = event.read_data()?.unwrap() {
            let tme = binlog_stream.get_tme(re.table_id());
            for row in re.rows(tme.unwrap()) {
                row.unwrap();
            }
        }
    }

    assert!(events_num > 0);
    timeout(Duration::from_secs(10), binlog_stream.close())
        .await
        .unwrap()
        .unwrap();
    println!("exit");

    Ok(())
}

// mysql-8.0.43/plugin/semisync/semisync_replica_plugin.cc:: repl_semi_slave_request_dump
// mariadb-10.3.39/sql/semisync_slave.cc:: request_transmit
