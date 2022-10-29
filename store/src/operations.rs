
use std::fmt;

use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{RecordBatchStream, DisplayFormatType};
use datafusion::physical_plan::metrics::MetricsSet;

use crate::datatypes::datetime_to_timestamp;
use crate::log::MAINLINE;
use crate::table::TableStore;
use crate::protocol;
use crate::writer::write_batches;
use crate::partition::split_batch;
use crate::result::CalicoResult;

// move to command pattern
// https://rust-unofficial.github.io/patterns/patterns/behavioural/command.html

pub trait Operation<T> {
    fn execute(&self, table_store: &TableStore) -> CalicoResult<T>;
    fn rollback(&self, table_store: &TableStore) -> CalicoResult<()>;

    /// Return a snapshot of the set of [`Metric`]s for this
    /// [`ExecutionPlan`].
    ///
    /// While the values of the metrics in the returned
    /// [`MetricsSet`]s may change as execution progresses, the
    /// specific metrics will not.
    ///
    /// Once `self.execute()` has returned (technically the future is
    /// resolved) for all available partitions, the set of metrics
    /// should be complete. If this function is called prior to
    /// `execute()` new metrics may appear in subsequent calls.
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Format this `ExecutionPlan` to `f` in the specified type.
    ///
    /// Should not include a newline
    ///
    /// Note this function prints a placeholder by default to preserve
    /// backwards compatibility.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExecutionPlan(PlaceHolder)")
    }
}

#[derive(Default, Debug)]
struct AppendOperation {
    application: String,
    committer: String,
    commit_message: String,

}

impl AppendOperation {
    fn with_batch(self, batch: &RecordBatch) -> AppendOperation {
        self
    }

/*    fn with_batch_stream(self, stream: &RecordBatchStream) -> AppendOperation {

    }
 */
    fn with_commit_message(self, commit_message: &String) -> AppendOperation {
        self
    }
}

pub async fn append_operation(table_store: &TableStore, 
                              batch:&RecordBatch) -> CalicoResult<Vec<protocol::Commit>> { 
    
    let split_batches = split_batch(table_store, batch).await?;
    let object_paths = write_batches(table_store, &split_batches).await?;

    let cols = vec!["a".to_string()];
    let col_expr = vec![("a".to_string(), "$new".to_string())];
    let mut commits = vec![];

    // TODO: handle distinct transaction logs
    let log = table_store.default_transaction_log().await?;
    let head_id = log.head_id_mainline().await?;

    // todo: move to params
    let timestamp = datetime_to_timestamp(&chrono::offset::Utc::now());

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

    let _new_head = log.fast_forward(MAINLINE, &commit.commit_id).await.unwrap();

    commits.push(commit.commit);


    Ok(commits)
}


struct CheckpointOperation {

}


struct RepartitionOperation {

}


struct SetColumnMetadataOperation {

}

struct SetColumnGroupMetadataOperation {

}


#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use tempfile::tempdir;
    use crate::operations::append_operation;
    use crate::test_util::*;

    #[tokio::test]
    async fn test_append() {
        let temp = tempdir().unwrap();
        let ctx = provision_ctx(temp.path());

        let col_groups = vec![COLGROUP_1, COLGROUP_2];
        let table_store = provision_store(&ctx, &col_groups).await;

        append_operation(&table_store, &make_data(10,0, 0,  &col_groups)).await.unwrap();
        append_operation(&table_store, &make_data(10,10, 0, &col_groups)).await.unwrap();
        append_operation(&table_store, &make_data(10, 20, 0, &col_groups)).await.unwrap();
        
        let log = table_store.default_transaction_log().await.unwrap();
        let head = log.head_mainline().await.unwrap();
        let history = head.history(100).await.unwrap();
        assert_eq!(history.len(), 3);
        // make sure the files are in the object store
    }
}