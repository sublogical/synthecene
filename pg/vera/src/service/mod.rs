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
    Dataspace,
    cell_value::Data,
};
use tonic::{Request, Response, Status};
use tracing::{info, instrument};

mod cql;
use self::cql::{
    derive_keyspace,
    derive_table_name,
    derive_update_schema,
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
