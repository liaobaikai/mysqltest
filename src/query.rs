use std::sync::LazyLock;

use mysql_async::{
    Conn,
    prelude::{FromRow, Queryable},
};
use regex::bytes::Regex;

#[derive(Debug, PartialEq, Eq, FromRow)]
pub struct Replica {
    server_id: u32,
    host: Option<String>,
    port: u16,
    // Alias: Source_id
    master_id: u32,
    replica_uuid: Option<String>,
}

impl Replica {
    #[allow(unused)]
    pub(crate) fn server_id(&self) -> u32 {
        self.server_id
    }
    #[allow(unused)]
    pub(crate) fn host(&self) -> Option<String> {
        self.host.clone()
    }
    #[allow(unused)]
    pub(crate) fn port(&self) -> u16 {
        self.port
    }
}

// Query:
// Old Version: show slave hosts
// New Version: show replicas
#[allow(unused)]
pub async fn show_replicas(
    conn: &mut Conn,
) -> std::result::Result<Vec<Replica>, mysql_async::Error> {
    let replicas = match conn.query_iter("show replicas").await {
        Ok(mut slaves) => {
            let mut replicas = Vec::new();
            let data = slaves
                .for_each(|row| {
                    replicas.push(Replica {
                        server_id: row.get(0).unwrap(),
                        host: row.get(1),
                        port: row.get(2).unwrap(),
                        master_id: row.get(3).unwrap(),
                        replica_uuid: None,
                    });
                })
                .await;
            replicas
        }
        Err(_) => match conn.query_iter("show slave hosts").await {
            Ok(mut slaves) => {
                let mut replicas = Vec::new();
                let data = slaves
                    .for_each(|row| {
                        replicas.push(Replica {
                            server_id: row.get(0).unwrap(),
                            host: row.get(1),
                            port: row.get(2).unwrap(),
                            master_id: row.get(3).unwrap(),
                            replica_uuid: None,
                        });
                    })
                    .await;
                replicas
            }
            Err(e) => return Err(e),
        },
    };
    Ok(replicas)
}

// Query:
// select @@rpl_semi_sync_master_enabled
pub async fn rpl_semi_sync_master_enabled(
    conn: &mut Conn,
) -> std::result::Result<usize, Box<dyn std::error::Error>> {
    let strings: Option<String> = conn
        .query_first("select @@rpl_semi_sync_master_enabled")
        .await?;
    // let enabled = strings.unwrap_or_default().parse::<usize>()?;
    Ok(strings.unwrap_or_default().parse::<usize>()?)
}

// Query:
// select @@server_id
pub async fn master_server_id(
    conn: &mut Conn,
) -> std::result::Result<u32, Box<dyn std::error::Error>> {
    let strings: Option<String> = conn.query_first("select @@server_id").await?;
    let server_id: u32 = strings.unwrap_or_default().parse()?;
    Ok(server_id)
}

// Query:
// select @@server_uuid
pub async fn master_server_uuid(
    conn: &mut Conn,
) -> std::result::Result<Option<String>, Box<dyn std::error::Error>> {
    let strings: Option<String> = conn.query_first("select @@server_uuid").await?;
    Ok(strings)
}

// for mysql, mariadb default true
// Query:
// select @@gtid_mode
//
// MySQL	5.6	    gtid_mode = ON
// MariaDB	10.0.2	gtid_strict_mode = ON
pub async fn master_gtid_mode(
    conn: &mut Conn,
) -> std::result::Result<bool, Box<dyn std::error::Error>> {
    let strings: Option<String> = conn.query_first("select @@gtid_mode").await?;
    Ok(strings != Some("OFF".to_owned()))
}

// for mysql, mariadb default true
// Query:
// select @@gtid_mode
//
// MySQL	5.6	    gtid_mode = ON
// MariaDB	10.0.2	gtid_strict_mode = ON
static MARIADB_VERSION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:5.5.5-)?(\d{1,2})\.(\d{1,2})\.(\d{1,3})-MariaDB").unwrap());
pub async fn is_mariadb(conn: &mut Conn) -> std::result::Result<bool, Box<dyn std::error::Error>> {
    let strings: Option<String> = conn.query_first("select version()").await?;
    Ok(MARIADB_VERSION_RE.is_match(strings.unwrap().as_bytes()))
}
