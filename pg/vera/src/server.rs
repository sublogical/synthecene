pub mod vera_api {
    tonic::include_proto!("vera_api"); // The string specified here must match the proto package name
}

use scylla::{Session, SessionBuilder};
use std::time::Duration;

use vera::vera_api::vera_server::VeraServer;
use tonic::{transport::Server};
use tracing::{info};

mod service;
use service::VeraService;

async fn healthcheck(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    let result = session
        .query_unpaged("SELECT keyspace_name FROM system_schema.keyspaces;", ())
        .await?
        .into_rows_result()?;

    if result.rows_num() == 0 {
        return Err("No keyspaces found".into());
    }

    // todo: check that the vera global keyspace exists
    // todo: check that the migrations have all been applied
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("vera=debug")
        .init();

    let uri = std::env::var("SCYLLA_URI")
        .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    info!("Connecting to ScyllaDB at {}", uri);
    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .connection_timeout(Duration::from_secs(3))
        .cluster_metadata_refresh_interval(Duration::from_secs(10))
        .build()
        .await?;
    info!("✅ ScyllaDB Connected");

    healthcheck(&session).await?;
    info!("✅ ScyllaDB Healthy");

    let addr = "[::1]:50053".parse()?;
    let vera = VeraService { session };
    info!("{} Starting VeraService on {:?}", "🦑", addr);
    Server::builder()
        .add_service(VeraServer::new(vera))
        .serve(addr)
        .await?;

    Ok(())
}
