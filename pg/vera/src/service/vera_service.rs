use scylla::Session;

use vera::vera_api::vera_server::Vera;
use vera::vera_api::{
    CreateTableRequest,
    CreateTableResponse,
    DeleteTableRequest,
    DeleteTableResponse,
    CreateColumnRequest,
    CreateColumnResponse,
    DeleteColumnRequest,
    DeleteColumnResponse,
    CreateTypeRequest,
    CreateTypeResponse,
    DeleteTypeRequest,
    DeleteTypeResponse,
    CreateUniverseRequest,
    CreateUniverseResponse,
    DeleteUniverseRequest,
    DeleteUniverseResponse,
    ReadDocumentsRequest,
    ReadDocumentsResponse,
    WriteDocumentsRequest, 
    WriteDocumentsResponse,
};
use tonic::{Request, Response, Status};
use tracing::{info, instrument};

use crate::service::cql::{
    derive_keyspace,
    derive_table_name,
    derive_table_create_schema,
    derive_update_schema,
    derive_values,
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
        let values = derive_values(&req.document_updates);

        info!("Writing to table: {:?}.{:?} with schema: {:?}", keyspace, table_name, schema);

        // todo: inspect table schema and add missing columns if necessary
        let query = format!("INSERT INTO {}.{} {} VALUES {}", keyspace, table_name, schema, values);

        info!("Query: {}", query);
        let _result = self.session
            .query_unpaged(query, ())
            .await
            .map_err(|e| Status::internal(e.to_string()))?; // todo: improve error handling

        info!("Table created: {:?}.{:?}", keyspace, table_name);

        Ok(Response::new(WriteDocumentsResponse {}))
    }

    #[instrument(skip(self))]
    async fn create_universe(
        &self,
        _request: Request<CreateUniverseRequest>,
    ) -> Result<Response<CreateUniverseResponse>, Status> {
        todo!("create universe");
    }

    #[instrument(skip(self))]
    async fn delete_universe(
        &self,
        _request: Request<DeleteUniverseRequest>,
    ) -> Result<Response<DeleteUniverseResponse>, Status> {
        todo!("delete universe");
    }

    #[instrument(skip(self))]
    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        let req = request.into_inner();

        // todo: check permissions of caller to create table in this universe

        let keyspace = derive_keyspace(&req.universe_uri);
        // todo: move this into a batch operation
        for table_spec in req.table_specs {
            let table_name = derive_table_name(&table_spec.table_uri);
            let schema = derive_table_create_schema(&table_spec.column_specs)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let query = format!("CREATE TABLE {}.{} ({})", keyspace, table_name, schema);

            // todo: add support for table configuration

            info!("Creating table: {:?}.{:?} with schema: {:?}", keyspace, table_name, schema);

            let _result = self.session
                .query_unpaged(query, ())
                .await
                .map_err(|e| Status::internal(e.to_string()))?; // todo: improve error handling

            // todo: add support for global table registry

            info!("Table created: {:?}.{:?}", keyspace, table_name);
        }

        Ok(Response::new(CreateTableResponse {}))
    }

    #[instrument(skip(self))]
    async fn delete_table(
        &self,
        request: Request<DeleteTableRequest>,
    ) -> Result<Response<DeleteTableResponse>, Status> {
        let req = request.into_inner();
        let keyspace = derive_keyspace(&req.universe_uri);
        let table_name = derive_table_name(&req.table_uri);

        // todo: check permissions of caller to create table in this universe

        if !req.delete_if_not_empty {
            let query = format!("SELECT document_uri FROM {}.{} LIMIT 1", keyspace, table_name);
            let result = self.session
                .query_unpaged(query, ())
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .into_rows_result()
                .map_err(|e| Status::internal(e.to_string()))?;

            if result.rows_num() > 0 {
                return Err(Status::failed_precondition("Table is not empty"));
            }
        }

        let query = format!("DROP TABLE {}.{}", keyspace, table_name);

        info!("Dropping table: {:?}.{:?}", keyspace, table_name);

        let _result = self.session
            .query_unpaged(query, ())
            .await
            .map_err(|e| Status::internal(e.to_string()))?; // todo: improve error handling

        info!("Table dropped: {:?}.{:?}", keyspace, table_name);

        Ok(Response::new(DeleteTableResponse {}))
    }

    #[instrument(skip(self))]
    async fn create_column(
        &self,
        request: Request<CreateColumnRequest>,
    ) -> Result<Response<CreateColumnResponse>, Status> {
        let req = request.into_inner();
        let _keyspace = derive_keyspace(&req.universe_uri);
        let _table_name = derive_table_name(&req.table_uri);


        todo!("create column");
    }

    #[instrument(skip(self))]
    async fn delete_column(
        &self,
        _request: Request<DeleteColumnRequest>,
    ) -> Result<Response<DeleteColumnResponse>, Status> {
        todo!("delete column");
    }

    #[instrument(skip(self))]
    async fn create_type(
        &self,
        _request: Request<CreateTypeRequest>,
    ) -> Result<Response<CreateTypeResponse>, Status> {
        todo!("create type");
    }

    #[instrument(skip(self))]
    async fn delete_type(
        &self,
        _request: Request<DeleteTypeRequest>,
    ) -> Result<Response<DeleteTypeResponse>, Status> {
        todo!("delete type");
    }
}
