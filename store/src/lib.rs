use std::sync::Arc;
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;

use crate::log::TransactionLog;
use result::CalicoResult;

pub mod protocol {
    // todo: not windows specific (gah)
    include!(concat!(env!("OUT_DIR"), "\\calico.protocol.rs"));
}

mod writer;

pub mod log;
//pub mod object;
pub mod operations;
pub mod partition;
// pub mod reader;

pub struct CalicoTable {
    object_store: Arc<Box<dyn ObjectStore>>,

    
}

impl CalicoTable {
    pub async fn load() -> CalicoResult<CalicoTable> {
        
        // load object_store for table metadata
        // for columns requested, get column-groups
        // load column-group metadata: partitioning, keyspace, all columns
        // lazy load object_stores & transaction logs for all tiles
        todo!()
    }

    pub async fn data_store_for(&self, tile: &protocol::Tile) -> CalicoResult<Arc<Box<dyn ObjectStore>>> {
        todo!()
    }

    pub async fn log_store_for(&self, tile: &protocol::Tile) -> CalicoResult<Arc<Box<dyn ObjectStore>>> {
        todo!()
    }

    pub async fn transaction_log_for(&self, tile: &protocol::Tile) -> CalicoResult<Arc<TransactionLog>> {
        todo!()
    }


    pub async fn object_path_for(&self, tile: &protocol::Tile) -> CalicoResult<ObjectStorePath> {
        todo!()
    }

    pub async fn column_group_for_column(&self, name: &str) -> CalicoResult<String> {
        todo!()
    }

    pub async fn column_group_meta(&self, column_group: &str) -> CalicoResult<protocol::ColumnGroupMetadata> {
        todo!()
    }

    pub async fn from_local(path: &std::path::Path) -> CalicoResult<CalicoTable> {
        todo!()
    }

    pub async fn add_column_group(&self, column_group_meta: protocol::ColumnGroupMetadata) -> CalicoResult<()> {
        todo!()
    }

    pub async fn add_column(&self, column_meta: protocol::ColumnMetadata) -> CalicoResult<()> {
        todo!()
    }

}

pub struct CalicoSchema {
}

pub mod result {
    use arrow::error::ArrowError;
    use parquet::errors::ParquetError;
    use prost::{DecodeError, EncodeError};
    use tokio::task::JoinError;
    use object_store::Error as ObjectStoreError;

    #[derive(Debug)]
    pub enum CalicoError {
        ArrowMessedUp(ArrowError),
        JoinFailed(JoinError),
        EncodeError(EncodeError),
        DecodeError(DecodeError),
        ObjectStoreError(ObjectStoreError),
        BranchNotFound(String),
        ParquetError(ParquetError),

        PartitionError(&'static str),
        TransactionLogAlreadyIntialized(String),
        TransactionLogNotPresent(String)
    }

    impl From<ArrowError> for CalicoError {
        fn from(err: ArrowError) -> Self {
            CalicoError::ArrowMessedUp(err)
        }
    }    
    impl From<JoinError> for CalicoError {
        fn from(err: JoinError) -> Self {
            CalicoError::JoinFailed(err)
        }
    }    
    impl From<EncodeError> for CalicoError {
        fn from(err: EncodeError) -> Self {
            CalicoError::EncodeError(err)
        }
    }    
    impl From<DecodeError> for CalicoError {
        fn from(err: DecodeError) -> Self {
            CalicoError::DecodeError(err)
        }
    }    
    impl From<ParquetError> for CalicoError {
        fn from(err: ParquetError) -> Self {
            CalicoError::ParquetError(err)
        }
    }    
    impl From<ObjectStoreError> for CalicoError {
        fn from(err: ObjectStoreError) -> Self {
            CalicoError::ObjectStoreError(err)
        }
    }    

    pub type CalicoResult<T> = Result<T, CalicoError>;
}
