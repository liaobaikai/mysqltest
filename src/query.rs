use mysql_async::{
    Conn,
    prelude::{FromRow, Queryable},
};

#[derive(Debug, PartialEq, Eq, FromRow)]
pub struct Replica {
    server_id: u64,
    host: Option<String>,
    port: u16,
    source_id: u64,
    replica_uuid: Option<String>,
}

impl Replica {
    pub(crate) fn server_id(&self) -> u64 {
        self.server_id
    }
}

// Query:
// Old Version: show slave hosts
// New Version: show replicas
pub async fn show_replicas(conn: &mut Conn) -> std::result::Result<Vec<Replica>, Box<dyn std::error::Error>> {
    let replicas = match conn.query("show replicas").await {
        Ok(replicas) => replicas,
        Err(_) => conn.query("show slave hosts").await?
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
) -> std::result::Result<u64, Box<dyn std::error::Error>> {
    let strings: Option<String> = conn
        .query_first("select @@server_id")
        .await?;
    let server_id: u64 = strings.unwrap_or_default().parse()?;
    Ok(server_id)
}

// Query:
// select @@server_uuid
pub async fn master_server_uuid(
    conn: &mut Conn,
) -> std::result::Result<Option<String>, Box<dyn std::error::Error>> {
    let strings: Option<String> = conn
        .query_first("select @@server_uuid")
        .await?;
    Ok(strings)
}

