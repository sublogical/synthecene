use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;

use crate::log::TransactionLog;
use crate::protocol;
use crate::result::{CalicoResult, CalicoError};

pub struct CalicoTable {
    object_store: Arc<Box<dyn ObjectStore>>,
    log: Arc<TransactionLog>,

    column_group_config: HashMap<String, protocol::ColumnGroupMetadata>,
    column_config: HashMap<String, protocol::ColumnMetadata>,
}

impl CalicoTable {
    pub async fn from_local(path: &std::path::Path) -> CalicoResult<CalicoTable> {
        let object_store:Arc<Box<dyn ObjectStore>> = Arc::new(Box::new(LocalFileSystem::new_with_prefix(path)?));
        let log = Arc::new(TransactionLog::init(object_store.clone()).await?);

        let table = CalicoTable {
            object_store,
            log,
            column_config: HashMap::new(),
            column_group_config: HashMap::new()
        };

        Ok(table)
    }

    pub async fn load() -> CalicoResult<CalicoTable> {
        
        // load object_store for table metadata
        // for columns requested, get column-groups
        // load column-group metadata: partitioning, keyspace, all columns
        // lazy load object_stores & transaction logs for all tiles
        todo!()
    }

    pub async fn data_store_for(&self, _tile: &protocol::Tile) -> CalicoResult<Arc<Box<dyn ObjectStore>>> {
        Ok(self.object_store.clone())
    }

    pub async fn log_store_for(&self, tile: &protocol::Tile) -> CalicoResult<Arc<Box<dyn ObjectStore>>> {
        Ok(self.object_store.clone())
    }

    pub async fn transaction_log_for(&self, _tile: &protocol::Tile) -> CalicoResult<Arc<TransactionLog>> {
        Ok(self.log.clone())
    }

    pub async fn object_path_for(&self, tile: &protocol::Tile) -> CalicoResult<ObjectStorePath> {
        todo!()
    }

    pub async fn column_group_for_column(&self, name: &str) -> CalicoResult<String> {
        todo!()
    }

    pub async fn column_group_meta(&self, column_group: &str) -> CalicoResult<&protocol::ColumnGroupMetadata> {
        self.column_group_config.get(column_group).ok_or(CalicoError::UnknownColumnGroup(column_group.to_string()))
    }

    pub async fn column_group(&self, column: &str) -> CalicoResult<&protocol::ColumnMetadata> {
        self.column_config.get(column).ok_or(CalicoError::UnknownColumn(column.to_string()))
    }

    pub async fn add_column_group(&mut self, column_group_meta: protocol::ColumnGroupMetadata) -> CalicoResult<()> {
        self.column_group_config.insert(column_group_meta.column_group.clone(), column_group_meta);
        Ok(())
    }

    pub async fn add_column(&mut self, column_meta: protocol::ColumnMetadata) -> CalicoResult<()> {
        self.column_config.insert(column_meta.column.clone(), column_meta);
        Ok(())
    }

}

