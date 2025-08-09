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
