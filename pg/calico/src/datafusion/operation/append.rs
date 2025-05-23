use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::Future;
use futures::stream::StreamExt;
use log::{debug};
use object_store::ObjectStore;
use uuid::Uuid;

use crate::log::{PendingCommit, MAIN};
use crate::partition::{TiledRecordBatchStream, stream_from_batches, stream_from_tiled_batches, partition_batch_stream, SendableRecordBatchStream, Tile};
use crate::table::TableStore;
use crate::protocol;
use crate::writer::{stream_batches_to_bytes, stream_bytes_to_objects};
use synthecene_shared::result::SyntheceneResult;

use super::Operation;

#[derive(Default, Debug)]
pub struct AppendOperation {
    pub pending_commit: PendingCommit,
    pub batch_data: BatchData,
    pub max_partfile_size: Option<usize>,
    pub start_partnum: Option<usize>,
    pub job_uuid: Option<Uuid>,
}

pub enum BatchData {
    None,
    Stream(SchemaRef, SendableRecordBatchStream),
    List(Vec<RecordBatch>),
    Direct(Vec<protocol::TileFiles>),
    TiledStream(TiledRecordBatchStream),
    TiledBatches(Vec<(protocol::Tile, Vec<RecordBatch>)>),
}

impl fmt::Debug for BatchData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchData::None => write!(f, "None"),
            BatchData::Stream(_,_) => write!(f, "Stream"),
            BatchData::List(v) => write!(f, "List({:?})", v.len()),
            BatchData::Direct(v) => write!(f, "Direct({:?})", v.len()),
            BatchData::TiledStream(_) => write!(f, "TiledStream"),
            BatchData::TiledBatches(v) => write!(f, "TiledBatches({:?})", v.len()),
        }
    }
}

impl Default for BatchData {
    fn default() -> Self {
        BatchData::None
    }
}

impl AppendOperation {
    pub fn init(parent_id: &Vec<u8>) -> AppendOperation {
        let pending_commit = PendingCommit::init(parent_id);

        Self {
            pending_commit,
            batch_data: BatchData::None,
            max_partfile_size: None,
            start_partnum: None,
            job_uuid: None,
        }
    }

    /// Adds a single unpartitioned record batch to the append operation
    /// 
    pub fn with_batch(mut self, batch: RecordBatch) -> AppendOperation {
        self.batch_data = BatchData::List(vec![batch]);
        self
    }

    pub fn with_batches(mut self, batches: Vec<RecordBatch>) -> AppendOperation {
        self.batch_data = BatchData::List(batches);
        self
    }

    pub fn with_partitioned_batches(mut self, batches: Vec<(protocol::Tile, Vec<RecordBatch>)>) -> AppendOperation {
        self.batch_data = BatchData::TiledBatches(batches);
        self
    }

    pub fn with_batch_stream(mut self, schema: SchemaRef, stream: SendableRecordBatchStream) -> AppendOperation {
        self.batch_data = BatchData::Stream(schema, stream);
        self
    } 

    pub fn with_partitioned_batch_stream(mut self, stream: TiledRecordBatchStream) -> AppendOperation {
        self.batch_data = BatchData::TiledStream(stream);
        self
    } 

    pub fn with_max_partfile_size(mut self, max_partfile_size: usize) -> AppendOperation {
        self.max_partfile_size = Some(max_partfile_size);
        self
    }

    pub fn with_start_partnum(mut self, start_partnum: usize) -> AppendOperation {
        self.start_partnum = Some(start_partnum);
        self
    }

    pub fn with_job_uuid(mut self, job_uuid: Uuid) -> AppendOperation {
        self.job_uuid = Some(job_uuid);
        self
    }

    pub fn with_commit_timestamp(mut self, commit_timestamp: u64) -> AppendOperation {
        self.pending_commit = self.pending_commit.with_commit_timestamp(commit_timestamp);
        self
    }
    pub fn with_commit_message(mut self, commit_message: &str) -> AppendOperation {
        self.pending_commit = self.pending_commit.with_commit_message(commit_message);
        self
    }

    pub fn with_committer(mut self, committer: &str) -> AppendOperation {
        self.pending_commit = self.pending_commit.with_committer(committer);
        self
    }

    pub fn with_application(mut self, application: &str) -> AppendOperation {
        self.pending_commit = self.pending_commit.with_application(application);
        self
    }
}

#[async_trait]
impl Operation<protocol::Commit> for AppendOperation {
    async fn execute(mut self, table_store: Arc<TableStore>) -> SyntheceneResult<protocol::Commit> {

        let max_partfile_size = self.max_partfile_size.unwrap_or(1024 * 1024 * 1024);
        let job_uuid = self.job_uuid.unwrap_or(Uuid::new_v4());
        let start_partnum = self.start_partnum.unwrap_or(0);
        
        let log = table_store.default_transaction_log().await?;    
        let object_store = table_store.default_object_store().await?;
        let store_path = "/".to_string();
        let commit_id = self.pending_commit.commit_id.clone();

        let all_tile_files:Vec<protocol::TileFiles> = match self.batch_data {
            BatchData::None => panic!("Should always have a stream or a list of tilefiles to commit"),
            BatchData::List(v) => {
                let stream = stream_from_batches(&v);
                let schema = v[0].schema();
                let tiled_stream = partition_batch_stream(table_store.clone(), schema, stream).await
                    .expect("failed to partition batch stream");

                tiled_stream_to_tile_files(
                    max_partfile_size,
                    object_store.clone(), 
                    store_path,
                    commit_id, 
                    job_uuid, 
                    start_partnum, 
                    tiled_stream).await
            },
            BatchData::Direct(files) => files.clone(),
            BatchData::Stream(schema, stream) => {
                let tiled_stream = partition_batch_stream(table_store.clone(), schema, stream).await
                    .expect("failed to partition batch stream");

                tiled_stream_to_tile_files(
                    max_partfile_size,
                    object_store.clone(), 
                    store_path,
                    commit_id, 
                    job_uuid, 
                    start_partnum, 
                    tiled_stream).await
            },
            BatchData::TiledStream(tiled_stream) => 
                tiled_stream_to_tile_files(
                    max_partfile_size,
                    object_store.clone(), 
                    store_path,
                    commit_id, 
                    job_uuid, 
                    start_partnum, 
                    tiled_stream).await,
            BatchData::TiledBatches(v) => {
                let tiled_stream = stream_from_tiled_batches(&v);
                tiled_stream_to_tile_files(
                    max_partfile_size,
                    object_store.clone(), 
                    store_path,
                    commit_id, 
                    job_uuid, 
                    start_partnum, 
                    tiled_stream).await
            }
        };

        self.pending_commit = self.pending_commit
            .with_tile_files(all_tile_files);

        let commit = self.pending_commit
            .commit(&log).await?;

        Ok(commit)
    }

    async fn abort(mut self, _table_store: Arc<TableStore>) -> SyntheceneResult<()> {
        // nothing to cleanup
        Ok(())
    }
}

fn stream_to_tile_files<'a, 'b: 'a>(
    max_partfile_size: usize,
    object_store: Arc<dyn ObjectStore>,
    store_path: String,
    commit_id: Vec<u8>,
    job_uuid: Uuid,
    start_partnum: usize,
    tile: Tile,
    schema: SchemaRef,
    stream: SendableRecordBatchStream
) -> impl Future<Output = Vec<protocol::TileFiles>> + 'b
{
    async move {
        let mut byte_stream = stream_batches_to_bytes(
            max_partfile_size,
            schema, 
            stream);

        let file_stream = stream_bytes_to_objects(
            &tile, 
            object_store.clone(),
            &store_path,
            &commit_id,
            &job_uuid,
            start_partnum, 
            &mut byte_stream);

        file_stream.collect::<Vec<protocol::TileFiles>>().await
    }
}

async fn tiled_stream_to_tile_files (
    max_partfile_size: usize,
    object_store: Arc<dyn ObjectStore>,
    store_path: String,
    commit_id: Vec<u8>,
    job_uuid: Uuid,
    start_partnum: usize,
    tiled_stream: TiledRecordBatchStream
) -> Vec<protocol::TileFiles> 
{
    let handles = tiled_stream.map(|result| 
        match result {
            Ok((tile, schema, batch_stream)) => {
                let store_path = store_path.clone();
                let object_store = object_store.clone();
                let commit_id = commit_id.clone();
                let job_uuid = job_uuid.clone();

                async move {
                    let tilename = format!("{:?}", tile);
                    
                    let tile_files = stream_to_tile_files(
                        max_partfile_size, 
                        object_store.clone(),
                        store_path,
                        commit_id,
                        job_uuid,
                        start_partnum,
                        tile,
                        schema, 
                        batch_stream).await;
                    debug!("Completed Stream for tile {:?}: {}", &tilename, tile_files.len());

                    tile_files
                }
            },

            Err(e) => panic!("Error: {:?}", e)
    }).map(|fut| {
        tokio::spawn(fut)
    }).collect::<Vec<_>>().await;
    
    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(handle.await.unwrap());
    }

    // flatten the results into a single vector
    let flattened = results.into_iter().flatten().collect();

    flattened
}

// todo: deprecate this and use the operation directly
pub async fn append_operation(
    table_store: Arc<TableStore>, 
    batch: RecordBatch
) -> SyntheceneResult<protocol::Commit> 
{
    let log = table_store.default_transaction_log().await?;
    let parent_id = log.head_id_main().await?;

    let commit = AppendOperation::init(&parent_id)
        .with_batch(batch)
        .execute(table_store).await?;

    let _new_head = log.fast_forward(MAIN, &commit.commit_id).await.unwrap();

    Ok(commit)
}

// todo: deprecate this and use the operation directly
pub async fn append_operation_at(
    table_store: Arc<TableStore>, 
    timestamp: u64,
    batch: RecordBatch
) -> SyntheceneResult<protocol::Commit>
{ 
    let log = table_store.default_transaction_log().await?;
    let parent_id = log.head_id_main().await?;

    let commit = AppendOperation::init(&parent_id)
        .with_batch(batch)
        .with_commit_timestamp(timestamp)
        .execute(table_store).await?;

    let _new_head = log.fast_forward(MAIN, &commit.commit_id).await.unwrap();

    Ok(commit)
}
    

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;
    use crate::datafusion::operation::append::{ AppendOperation, append_operation };
    use crate::datafusion::operation::Operation;
    use crate::log::MAIN;
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
        let head = log.head_main().await.unwrap();
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

        let log = table_store.default_transaction_log().await.unwrap();

        let parent_id = log.head_id_main().await.unwrap();
    
        let commit = AppendOperation::init(&parent_id)
            .with_batch(batch)
            .with_commit_message("first commit")
            .execute(table_store.clone()).await.unwrap();

        let _new_ref = log.fast_forward(MAIN, &commit.commit_id).await.unwrap();
        let new_head = log.head_main().await.unwrap();

        let history = new_head.history(100).await.unwrap();
        assert_eq!(history.len(), 1);
        // make sure the files are in the object store
    }

    use object_store::path::Path as ObjectStorePath;

    #[tokio::test]
    async fn test_list_small_batches() {
        let temp = tempdir().unwrap();
        let ctx = provision_ctx(temp.path());

        let col_groups = vec![COLGROUP_2];
        let table_store = Arc::new(provision_store(&ctx, &col_groups).await);
        let batches = (0..10).map(|i| {
            make_data(1_000, i * 1_000, 0,  &col_groups)
        }).collect::<Vec<_>>();

        let log = table_store.default_transaction_log().await.unwrap();
        let parent_id = log.head_id_main().await.unwrap();
    
        let commit = AppendOperation::init(&parent_id)
            .with_batches(batches)
            .with_commit_message("first commit")
            .execute(table_store.clone()).await.unwrap();

        let _new_ref = log.fast_forward(MAIN, &commit.commit_id).await.unwrap();
        let new_head = log.head_main().await.unwrap();

        let history = new_head.history(100).await.unwrap();
        assert_eq!(history.len(), 1);

        // make sure all the batches were combined into a single file
        assert_eq!(history[0].tile_files.len(), 1);

        // make sure the file is in the object store
        let object_store = table_store.default_object_store().await.unwrap();
        let file_path:ObjectStorePath = history[0].tile_files[0].file[0].file_path.clone().try_into().unwrap();
        let meta = object_store.head(&file_path).await.unwrap();

        assert!(meta.size > 0);
    }


    #[tokio::test]
    async fn test_mega_append() {
        let temp = tempdir().unwrap();
        let ctx = provision_ctx(temp.path());

        let col_groups = vec![COLGROUP_1, COLGROUP_2];
        let table_store = Arc::new(provision_store(&ctx, &col_groups).await);
/*
        let batch_stream = Box::pin(stream! {
            let col_groups = vec![COLGROUP_1, COLGROUP_2];

            for i in 0..10 {
                let batch = make_data(1_000_000, i * 10_000_000, 0,  &col_groups);
                yield batch;
            }
        });
 */
        // todo: make this use a stream
        // todo: make this count the size & calculate bytes per second
        // todo: make this test the size boundaries
        // todo: make this reconstruct the files and verify the data

        let batch = make_data(1_000, 0, 0,  &col_groups);
        let log = table_store.default_transaction_log().await.unwrap();

        let parent_id = log.head_id_main().await.unwrap();
    
        let commit = AppendOperation::init(&parent_id)
            .with_batch(batch)
            .with_commit_message("first commit")
            .with_max_partfile_size(1_000)
            .execute(table_store.clone()).await.unwrap();

        let _new_ref = log.fast_forward(MAIN, &commit.commit_id).await.unwrap();
        let new_head = log.head_main().await.unwrap();

        let history = new_head.history(100).await.unwrap();
        assert_eq!(history.len(), 1);

        // make sure we got more than one file per partition
        assert!(history[0].tile_files.len() > 3);

        for file in history[0].tile_files.iter() {
            assert!(file.file.len() < 1_500);
        }
    }



}