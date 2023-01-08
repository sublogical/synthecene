use std::fmt;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_plan::{SendableRecordBatchStream};

use crate::datatypes::datetime_to_timestamp;
use crate::log::MAINLINE;
use crate::table::TableStore;
use crate::protocol;
use crate::writer::write_batches;
use crate::partition::split_batch;
use calico_shared::result::CalicoResult;

use super::Operation;

#[derive(Default, Debug)]
pub struct AppendOperation {
    pub application: String,
    pub committer: String,
    pub commit_message: String,
    pub batch_data: BatchData,
    pub timestamp: Option<u64>
}

pub enum BatchData {
    None,
    Stream(SendableRecordBatchStream),
    List(Vec<RecordBatch>)
}

impl fmt::Debug for BatchData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchData::None => write!(f, "None"),
            BatchData::Stream(_) => write!(f, "RecordBatchStream"),
            BatchData::List(v) => write!(f, "RecordBatchList({:?})", v.len())
        }
    }
}

impl Default for BatchData {
    fn default() -> Self {
        BatchData::None
    }
}

impl AppendOperation {
    pub fn from_batch(batch: RecordBatch) -> AppendOperation {
        Self::default().with_batch(batch)
    }

    pub fn with_batch(mut self, batch: RecordBatch) -> AppendOperation {
        self.batch_data = BatchData::List(vec![batch]);
        self
    }

    pub fn with_batches(mut self, batches: Vec<RecordBatch>) -> AppendOperation {
        self.batch_data = BatchData::List(batches);
        self
    }

    pub fn with_batch_stream(mut self, stream: SendableRecordBatchStream) -> AppendOperation {
        self.batch_data = BatchData::Stream(stream);
        self
    } 

    pub fn with_timestamp(mut self, timestamp: u64) -> AppendOperation {
        self.timestamp = Some(timestamp);
        self
    }
    pub fn with_commit_message(mut self, commit_message: &str) -> AppendOperation {
        self.commit_message = commit_message.to_string();
        self
    }

    pub fn with_committer(mut self, committer: &str) -> AppendOperation {
        self.committer = committer.to_string();
        self
    }

    pub fn with_application(mut self, application: &str) -> AppendOperation {
        self.application = application.to_string();
        self
    }
}

#[async_trait]
impl Operation<protocol::Commit> for AppendOperation {
    async fn execute(&mut self, table_store: Arc<TableStore>) -> CalicoResult<protocol::Commit> {

        let batch = match &self.batch_data {
            BatchData::None => panic!("Should always have a stream or a list of records to commit"),
            BatchData::Stream(_) => todo!("Streaming support not yet implemented"),
            BatchData::List(v) => {
                assert_eq!(v.len(), 1);
                &v[0]
            },
        };

        let split_batches = split_batch(&table_store, &batch).await?;
        let object_paths = write_batches(&table_store, &split_batches).await?;
    
        // todo: track the actual columns mutated
        let cols = vec!["a".to_string()];
        let col_expr = vec![("a".to_string(), "$new".to_string())];
    
        // TODO: handle distinct transaction logs
        let log = table_store.default_transaction_log().await?;
        let head_id = log.head_id_mainline().await?;
    
        let timestamp = match self.timestamp {
            Some(ts) => ts,
            None => datetime_to_timestamp(&chrono::offset::Utc::now())
        };
    
        let mut all_tile_files = vec![];
    
        for (tile, file) in object_paths {
            let tile_files = protocol::TileFiles {
                tile: Some(tile),
                file: vec![file]
            };
    
            all_tile_files.push(tile_files);
        }
    
        let commit = log.create_commit(
            &head_id.to_vec(), 
            None, 
            None, 
            None, 
            timestamp, 
            cols.to_vec(), 
            col_expr.to_vec(), 
            all_tile_files).await?;
    
        let _new_head = log.fast_forward(MAINLINE, &commit.commit_id).await?;
    
        Ok(commit.commit)
    }

    async fn abort(&mut self, _table_store: Arc<TableStore>) -> CalicoResult<()> {
        todo!()
    }
}

pub async fn append_operation(table_store: Arc<TableStore>, 
                              batch: RecordBatch) -> CalicoResult<protocol::Commit> { 
    AppendOperation::from_batch(batch).execute(table_store).await
}

pub async fn append_operation_at(table_store: Arc<TableStore>, 
                                 timestamp: u64,
                                 batch: RecordBatch) -> CalicoResult<protocol::Commit> { 
    AppendOperation::from_batch(batch)
        .with_timestamp(timestamp)
        .execute(table_store).await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;
    use crate::operation::append::{ AppendOperation, append_operation };
    use crate::operation::Operation;
    use crate::test_util::*;

    #[tokio::test]
    async fn test_append() {
        let temp = tempdir().unwrap();
        let ctx = provision_ctx(temp.path());

        let col_groups = vec![COLGROUP_1, COLGROUP_2];
        let table_store = Arc::new(provision_store(&ctx, &col_groups).await);

        append_operation(table_store.clone(), make_data(10,0, 0,  &col_groups)).await.unwrap();
        append_operation(table_store.clone(), make_data(10,10, 0, &col_groups)).await.unwrap();
        append_operation(table_store.clone(), make_data(10, 20, 0, &col_groups)).await.unwrap();
        
        let log = table_store.default_transaction_log().await.unwrap();
        let head = log.head_mainline().await.unwrap();
        let history = head.history(100).await.unwrap();
        assert_eq!(history.len(), 3);
        // make sure the files are in the object store
    }
    
    #[tokio::test]
    async fn test_basic_append() {
        let temp = tempdir().unwrap();
        let ctx = provision_ctx(temp.path());

        let col_groups = vec![COLGROUP_1];
        let table_store = Arc::new(provision_store(&ctx, &col_groups).await);

        let batch = build_table(
            &vec![
                (ID_FIELD, i32_col(&vec![0, 1, 2])),
                (FIELD_A, i32_col(&vec![11, 12, 13])),
                (FIELD_B, i32_col(&vec![21, 22, 23])),
            ]
        );

        AppendOperation::from_batch(batch)
            .with_commit_message("first commit")
            .execute(table_store.clone()).await.unwrap();
        
        let log = table_store.default_transaction_log().await.unwrap();
        let head = log.head_mainline().await.unwrap();
        let history = head.history(100).await.unwrap();
        assert_eq!(history.len(), 1);
        // make sure the files are in the object store
    }




}