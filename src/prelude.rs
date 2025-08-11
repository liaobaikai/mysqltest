use clap::Parser;
use serde::Deserialize;

pub const NAME: &str = env!("CARGO_PKG_NAME");

#[derive(Deserialize, Clone)]
pub struct MyConfig {
    pub mysql: MyConfigForMySQL,
    pub replica: MyConfigForReplica,
}

#[derive(Deserialize, Clone)]
pub struct MyConfigForMySQL {
    pub host: String,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub port: Option<u16>,
    pub start_position: Option<u64>,
    pub start_filename: Option<String>,
    pub max_allowed_packet: Option<usize>,
    pub init_sql: Option<Vec<String>>,
}

#[derive(Deserialize, Clone)]
pub struct MyConfigForReplica {

    pub replica_id: Option<u32>,
    pub replica_uuid: Option<String>,
    pub report_port: Option<u16>,
    pub report_host: Option<String>,

    pub datadir: String,
    // Default Value: datadir + '/' + 'relay-bin'
    pub relay_log_basename: Option<String>,
    // Default Value: relay-bin
    pub relay_log: Option<String>,
    // Default Value: relay-bin.index
    pub relay_log_index: Option<String>,
    // Default Value: 1
    #[allow(unused)]
    pub relay_log_purge: Option<usize>,
    // Default Value: 1
    #[allow(unused)]
    pub relay_log_recovery: Option<usize>,
    // Default Value: 0
    pub rpl_semi_sync_replica_enabled: Option<usize>,
    // Default Value: 1024
    #[allow(unused)]
    pub replica_max_allowed_packet: Option<usize>,
    // Default Value: 1
    pub replica_verify_checksum: Option<usize>,
    // skip_event = "ROTATE_EVENT"
    pub skip_event: Option<Vec<u8>>,
}

// https://docs.rs/clap/latest/clap/_derive/index.html
#[derive(Parser, Clone, Debug)]
#[command(about, long_about = None)]
pub struct Args {
    /// Get the binlog from server.
    #[arg(short='H', long)]
    pub host: Option<String>,

    /// Connect to the remote server as username.
    #[arg(short, long)]
    pub user: Option<String>,

    /// Password to connect to remote server.
    #[arg(short, long)]
    pub password: Option<String>,

    /// Port number to use for connection or built-in default (3306).
    #[arg(short = 'P', long)]
    pub port: Option<u16>,

    /// Verify checksum binlog events.
    #[arg(long, default_value_t = false)]
    pub verify_binlog_checksum: bool,

    /// Start reading the binlog at position N. Applies to the first binlog passed on the command line.
    #[arg(short = 'j', long)]
    pub start_position: Option<u64>,

    /// Stop reading the binlog at position N. Applies to the first binlog passed on the command line.
    #[arg(long)]
    pub stop_position: Option<u64>,

    #[arg(short = 'F', long, default_value_t = true)]
    pub with_file: bool,

    // #[arg(short = 'G', long, default_value_t = false)]
    // pub with_gtid: bool,

    /// Start reading the binlog filename
    #[arg(long)]
    pub start_filename: Option<String>,
}

