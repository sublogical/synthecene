pub mod vera_api {
    tonic::include_proto!("vera_api"); // The string specified here must match the proto package name
}

use itertools::Itertools;
use scylla::{Session, SessionBuilder};
use std::time::Duration;

use vera_api::vera_server::{ Vera, VeraServer };
use vera_api::{
    CreateTableRequest,
    CreateTableResponse,
    CreateColumnRequest,
    CreateColumnResponse,
    DeleteColumnRequest,
    DeleteColumnResponse,
    ReadDocumentsRequest,
    ReadDocumentsResponse,
    WriteDocumentsRequest, 
    WriteDocumentsResponse,
    DocumentUpdate,
    CellValue,
    ColumnSpec,
    Dataspace,
    cell_value::Data,
};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, instrument};

#[derive(Debug, Default)]
pub struct VeraService {}

fn derive_keyspace(dataspace: &Dataspace) -> String {
    format!("{}_universe", escape_uri(&dataspace.universe))
}

fn derive_table_name(dataspace: &Dataspace) -> String {
    format!("{}_version_{}", escape_uri(&dataspace.table), escape_uri(&dataspace.version))
}

fn derive_update_schema(column_uris: &[String]) -> String {
    let mut schema = String::new();

    // Create a comma-separated list of escaped column URIs
    let columns: Vec<String> = column_uris
        .iter()
        .map(|uri| escape_uri(uri))
        .collect();
    
    // Join them with commas and wrap in a single set of parentheses
    schema.push('(');
    schema.push_str("doc_id");
    if !columns.is_empty() {
        schema.push_str(", ");
        schema.push_str(&columns.join(", "));
    }
    schema.push(')');

    schema
}

/**
 * Generate a schema for a duplicate update in a 
 * "INSERT ... ON DUPLICATE KEY UPDATE" statement
 *
 * E.g. id=VALUES(id), a=VALUES(a), b=VALUES(b), c=VALUES(c)
 */
fn derive_duplicate_update_schema(column_uris: &[String]) -> String {
    let mut schema = String::new();

    // Create a comma-separated list of escaped column URIs
    let columns: Vec<String> = column_uris
        .iter()
        .map(|uri| format!("{c} = VALUES({c})", c = escape_uri(uri)))
        .collect();

    schema.push_str("doc_id = VALUES(doc_id), ");
    schema.push_str(&columns.join(", "));

    schema
}

fn to_cql_value(value: &CellValue) -> String {
    // todo: add column_spec and validate type vs value
    // todo: add support for set, list and map types
    // todo: add support for append, prepend, add and remove operations on complex types
    match &value.data {
        Some(Data::StringValue(s)) => format!("'{}'", s),
        Some(Data::Int64Value(i)) => i.to_string(),
        Some(Data::DoubleValue(f)) => f.to_string(),
        _ => todo!("handle other data types"),
    }
}

/**
 * Generate a comma-separated list of values for a "INSERT ... ON DUPLICATE KEY UPDATE" statement
 *
 * E.g. (1, 'a1', 'b1', 'c1'), (2, 'a2', 'b2', 'c2'), (3, 'a3', 'b3', 'c3')
 */
fn derive_values(document_updates: &[DocumentUpdate]) -> String {

    let values = document_updates
        .iter()
        .map(|update| {
            let mut values = String::new();
            let cell_values: Vec<String> = update.cell_values
                .iter()
                .map(|value| to_cql_value(value))
                .collect();

            values.push_str(&format!("({}, ", update.document_id));
            values.push_str(&cell_values.join(", "));
            values.push_str(")");
            values
        })
        .join(", ");

    values
}

fn escape_uri(input: &str) -> String {
    let escape_count = input.chars().filter(|c| !c.is_alphanumeric() && *c != '_').count();

    let mut result = String::with_capacity(input.len()+escape_count);
    for c in input.chars() {
        if c.is_alphanumeric() || c == '_' {
            result.push(c);
        } else {
            result.push_str("__");
        }
    }
    result
}

#[tonic::async_trait]
impl Vera for VeraService {
    #[instrument(skip(self))]
    async fn get(
        &self,
        _request: Request<ReadDocumentsRequest>, // Accept request of type MetricRequest
    ) -> Result<Response<ReadDocumentsResponse>, Status> { // Return an instance of type MetricResponse
        todo!("read from cassandra");
    }


    #[instrument(skip(self))]
    async fn put(
        &self,
        request: Request<WriteDocumentsRequest>, // Accept request of type MetricRequest
    ) -> Result<Response<WriteDocumentsResponse>, Status> { // Return an instance of type MetricResponse
        let req = request.into_inner();

        let dataspace = req.dataspace.clone().ok_or(Status::invalid_argument("dataspace is required"))?;

        // todo: use dataspace universe to decide what cassandra instance to use

        let keyspace = derive_keyspace(&dataspace);
        let table_name = derive_table_name(&dataspace);
        let schema = derive_update_schema(&req.column_uris);

        info!("Writing to table: {:?}.{:?} with schema: {:?}", keyspace, table_name, schema);

        // todo: inspect table schema and add missing columns if necessary

        /* 
            INSERT INTO mytable (id, a, b, c)
            VALUES (1, 'a1', 'b1', 'c1'),
            (2, 'a2', 'b2', 'c2'),
            (3, 'a3', 'b3', 'c3'),
            (4, 'a4', 'b4', 'c4'),
            (5, 'a5', 'b5', 'c5'),
            (6, 'a6', 'b6', 'c6')
            ON DUPLICATE KEY UPDATE id=VALUES(id),
            a=VALUES(a),
            b=VALUES(b),
            c=VALUES(c);
        */

        info!("Writing to dataspace: {:?}", dataspace);
        todo!("write to cassandra");
    }

    #[instrument(skip(self))]
    async fn create_table(
        &self,
        _request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        todo!("create table");
    }

    #[instrument(skip(self))]
    async fn create_column(
        &self,
        _request: Request<CreateColumnRequest>,
    ) -> Result<Response<CreateColumnResponse>, Status> {
        todo!("create column");
    }

    #[instrument(skip(self))]
    async fn delete_column(
        &self,
        _request: Request<DeleteColumnRequest>,
    ) -> Result<Response<DeleteColumnResponse>, Status> {
        todo!("delete column");
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

    let addr = "[::1]:50053".parse()?;
    let vera = VeraService::default();
    println!("{} Starting VeraService on {:?}", "🦑", addr);
    Server::builder()
        .add_service(VeraServer::new(vera))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test) ]
mod tests { 
    use super::*;
    fn generate_dataspace(universe: &str, table: &str, version: &str) -> Dataspace {
        Dataspace {
            universe: universe.to_string(),
            table: table.to_string(),
            version: version.to_string(),
        }
    }

    

    fn make_column_spec(column_uri: &str, type_uri: &str) -> ColumnSpec {
        ColumnSpec {
            column_uri: column_uri.to_string(),
            type_uri: type_uri.to_string(),
        }
    }


    #[test]
    fn test_escape_uri() {
        assert_eq!(escape_uri("hello-world"), "hello__world");
        assert_eq!(escape_uri("hello_world"), "hello_world");
        assert_eq!(escape_uri("/hello/world#foo"), "__hello__world__foo");
    }

    #[test]
    fn test_derive_keyspace() {
        assert_eq!(derive_keyspace(&generate_dataspace("test", "t1", "1")), "test_universe");
        assert_eq!(derive_keyspace(&generate_dataspace("foo/bar#howdy", "t2", "1")), "foo__bar__howdy_universe");
    }

    #[test]
    fn test_derive_table_name() {
        assert_eq!(derive_table_name(&generate_dataspace("test", "test", "1.0.1")), "test_version_1__0__1");
        assert_eq!(derive_table_name(&generate_dataspace("test", "foo/bar#howdy", "v2")), "foo__bar__howdy_version_v2");
    }

    #[test]
    fn test_derive_update_schema() {
        let column_uris = vec![
            "__std__text_prompt".to_string(),
            "__std__my_foo".to_string(),
            "__std__my_bar".to_string(),
        ];

        assert_eq!(
            derive_update_schema(&column_uris),
            "(doc_id, __std__text_prompt, __std__my_foo, __std__my_bar)"
        );
    }

    fn make_document_update(document_id: &str, cell_values: Vec<CellValue>) -> DocumentUpdate {
        DocumentUpdate {
            document_id: document_id.to_string(),
            cell_values: cell_values,
        }
    }
    fn make_string_cell_value(value: &str) -> CellValue {
        CellValue {
            data: Some(Data::StringValue(value.to_string())),
        }
    }

    fn make_int_cell_value(value: i64) -> CellValue {
        CellValue {
            data: Some(Data::Int64Value(value)),
        }
    }

    fn make_double_cell_value(value: f64) -> CellValue {
        CellValue {
            data: Some(Data::DoubleValue(value)),
        }
    }

    #[test]
    fn test_derive_values() {
        let document_updates = vec![
            make_document_update("1", vec![
                make_string_cell_value("alpha"),
                make_string_cell_value("beta"),
                make_int_cell_value(1),
                make_double_cell_value(2.0),
            ]),
            make_document_update("2", vec![
                make_string_cell_value("delta"),
                make_string_cell_value("epsilon"),
                make_int_cell_value(3),
                make_double_cell_value(4.15),
            ]),
        ];

        assert_eq!(derive_values(&document_updates), "(1, 'alpha', 'beta', 1, 2), (2, 'delta', 'epsilon', 3, 4.15)");
    }
}     
