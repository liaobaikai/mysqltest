use clap::Parser;
use serde::Deserialize;

pub const NAME: &str = env!("CARGO_PKG_NAME");

#[derive(Deserialize)]
pub struct MyConfig {
    pub master: MyConfigMaster,
    pub local: MyConfigLocal,
}

#[derive(Deserialize)]
pub struct MyConfigMaster {
    pub host: String,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub port: Option<u16>,
    pub start_position: Option<u64>,
    pub start_filename: Option<String>,
    pub max_allowed_packet: Option<usize>,
    pub init_sql: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub struct MyConfigLocal {
    pub rpl_semi_sync_slave_enabled: Option<usize>,
    // 自动生成
    pub slave_id: Option<u32>,
    pub slave_uuid: Option<String>,
    pub register_slave_port: Option<u16>,
    pub register_slave_host: Option<String>,
    pub datadir: String,
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
    #[arg(short = 'P', long, default_value_t = 3306)]
    pub port: u16,

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


// pub fn init_logger() -> std::result::Result<(), Box<dyn std::error::Error>> {
//     // let stdout = ConsoleAppender::builder().build();
//     let logpath = Path::new("log").join(format!("{}.log", NAME));
//     let file_appender = FileAppender::builder()
//         .encoder(Box::new(PatternEncoder::new(
//             "{d(%m %d %H:%M:%S%.3f)} {l:>5.5} {({M}:{L}):20.20}: {m}{n}",
//         )))
//         .build(logpath)?;

//     let config = Config::builder()
//         .appender(Appender::builder().build("file", Box::new(file_appender)))
//         .build(Root::builder().appender("file").build(LevelFilter::Info))
//         .unwrap();

//     log4rs::init_config(config)?;
//     Ok(())
// }
