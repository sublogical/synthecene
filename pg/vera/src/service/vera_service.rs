use itertools::Itertools;
use scylla::{Session };

use vera::vera_api::vera_server::{ Vera, VeraServer };
use vera::vera_api::{
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
    cell_value::Data,
};
use tonic::{Request, Response, Status};
use tracing::{info, instrument};

use crate::service::cql::{
    derive_keyspace,
    derive_table_name,
    derive_update_schema,
    derive_table_create_schema,
};

#[derive(Debug)]
pub struct VeraService {
    pub session: Session,
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

        // todo: use universe to decide what cassandra instance to use

        let keyspace = derive_keyspace(&req.universe_uri);
        let table_name = derive_table_name(&req.table_uri);
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

        todo!("write to cassandra");
    }

    #[instrument(skip(self))]
    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        info!("Creating table: {:?}", request);
        let req = request.into_inner();

        // todo: check permissions of caller to create table in this universe

        let keyspace = derive_keyspace(&req.universe_uri);
        let table_name = derive_table_name(&req.table_uri);
        let schema = derive_table_create_schema(&req.column_specs)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let query = format!("CREATE TABLE {}.{} ({})", keyspace, table_name, schema);

        // todo: add support for table configuration

        info!("Creating table: {:?}.{:?} with schema: {:?}", keyspace, table_name, schema);

        let result = self.session
            .query_unpaged(query, ())
            .await
            .map_err(|e| Status::internal(e.to_string()))?; // todo: improve error handling

        info!("Table created: {:?}.{:?}", keyspace, table_name);

        Ok(Response::new(CreateTableResponse {}))
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
