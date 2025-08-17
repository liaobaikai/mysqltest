use std::{fs::File, io::Read, path::Path, process::exit};

use clap::Parser;
use serde::Deserialize;
use uuid::Uuid;

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


pub fn prelude() -> std::result::Result<(MyConfig, Args, String, u64), Box<dyn std::error::Error>> {

    let args = Args::parse();
    
    let config_path = option_env!("MYSQL_AVATAR_CONFIG_FILE").unwrap_or("./config.toml");
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

    let log_file_name = match args.start_filename.clone() {
        Some(v) => v,
        None => match config.mysql.start_filename.clone() {
            Some(v) if !v.is_empty() => v,
            _ => {
                println!("Using --with-file, --start-filename is requred");
                exit(1);
            }
        },
    };

    log::info!("Start_filename={:?}", log_file_name);
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
        "Replica_id={}",
        match config.replica.replica_id.clone() {
            Some(v) => format!("{}", v),
            None => format!("<Not set>"),
        }
    );

    log::info!("Replica_uuid={:?}", replica_uuid);

    log::info!(
        "Relay_log_basename={:?}",
        config.replica.relay_log_basename.clone().unwrap()
    );

    if config.replica.relay_log.clone().is_none() {
        config.replica.relay_log = Some(String::from("relay-bin"))
    }

    log::info!("Relay_log={:?}", config.replica.relay_log.clone().unwrap());
    if config.replica.relay_log_index.clone().is_none() {
        config.replica.relay_log_index = Some(String::from("relay-bin.index"))
    }
    log::info!(
        "Relay_log_index={:?}",
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

    log::info!(
        "Rpl_semi_sync_replica_enabled={}",
        rpl_semi_sync_replica_enabled
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

    let sqls = if let Some(mut init_sql) = config.mysql.init_sql.clone() {
        init_sql.append(&mut init);
        init_sql
    } else {
        init
    };

    config.mysql.init_sql = Some(sqls);

    Ok((config, args.clone(), log_file_name, log_file_pos))

}