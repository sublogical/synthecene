pub mod vera_api {
    tonic::include_proto!("vera_api"); // The string specified here must match the proto package name
}

use scylla::{Session, SessionBuilder};
use std::time::Duration;

use vera_api::vera_server::{ Vera, VeraServer };
use vera_api::{
    ReadDocumentsRequest,
    ReadDocumentsResponse,
    WriteDocumentsRequest, 
    WriteDocumentsResponse,
    Property,
    PropertyValue,
    PropertySpec,
    Dataspace,
    property_value::Data,
};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, warn, error, instrument};

#[derive(Debug, Default)]
pub struct VeraService {}

fn get_table_name(dataspace: &Dataspace) -> String {
    format!("{}.{}", dataspace.table, dataspace.version)
}

fn schema_type_for_value(value: &PropertyValue) -> Result<String, Status> {
    match value.data {
        Some(Data(StringValue(s))) => Ok(s.clone()),
        Some(Data(Int64Value(i))) => Ok(i.to_string()),
        Some(Data(DoubleValue(f))) => Ok(f.to_string()),
    }
}
fn derive_schema(properties: &[Property]) -> Result<String, Status> {
    let mut schema = String::new();

    for property in properties {
        let spec = property.spec.ok_or(Status::invalid_argument("spec is required"))?;
        let property_id = spec.property_id.ok_or(Status::invalid_argument("property_id is required"))?;

        let schema_type = schema_type_for_value(&property.value)?;
        schema.push_str(&format!("{}: {}\n", property_id, schema_type));
    }

    Ok(schema)
}

#[tonic::async_trait]
impl Vera for VeraService {
    #[instrument(skip(self))]
    async fn get(
        &self,
        request: Request<ReadDocumentsRequest>, // Accept request of type MetricRequest
    ) -> Result<Response<ReadDocumentsResponse>, Status> { // Return an instance of type MetricResponse
        let response = ReadDocumentsResponse {};

        Ok(Response::new(response))
    }

    #[instrument(skip(self))]
    async fn put(
        &self,
        request: Request<WriteDocumentsRequest>, // Accept request of type MetricRequest
    ) -> Result<Response<WriteDocumentsResponse>, Status> { // Return an instance of type MetricResponse
        let req = request.into_inner();

        let dataspace = req.dataspace.ok_or(Status::invalid_argument("dataspace is required"))?;

        // todo: use dataspace universe to decide what cassandra instance to use

        let table_name = get_table_name(dataspace)?;
        let schema = derive_schema(&req.properties)?;

        let table_name = format!("{}.{}", dataspace.universe, dataspace.table);

        let table = dataspace.table;
        let version = dataspace.version;

        let uri = std::env::var("SCYLLA_URI")
        .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

        for write_doc in req.write_docs {
            info!(
                versioned_doc_id = %write_doc.versioned_doc_id,
       
                "Write doc: {:?}", write_doc);
        }

        let 


        info!(
            agent_id = %req.agent_id,
            key = %req.key,
            value = %req.value,
            "Setting property"
        );

        let response: WriteDocumentsResponse = WriteDocumentsResponse {};


        Ok(Response::new(response))
    }
}

async fn healthcheck() -> Result<(), Box<dyn std::error::Error>> {
    let uri = std::env::var("SCYLLA_URI")
        .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting Session");
    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .connection_timeout(Duration::from_secs(3))
        .cluster_metadata_refresh_interval(Duration::from_secs(10))
        .build()
        .await?;
    
    println!("Session Connected");
    let result = session
        .query_unpaged("SELECT keyspace_name FROM system_schema.keyspaces;", ())
        .await?
        .into_rows_result()?;
        
    println!("KEYSPACES");
    for row in result.rows::<(Option<&str>,)>()? {
        let (keyspace_name,): (Option<&str>,) = row?;
        println!("{}", keyspace_name.unwrap_or("NONE"));
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    healthcheck().await?;

    let addr = "[::1]:50052".parse()?;
    let vera = VeraService::default();
    println!("{} Starting VeraService on {:?}", "🦑", addr);
    Server::builder()
        .add_service(VeraServer::new(vera))
        .serve(addr)
        .await?;

    Ok(())
}
