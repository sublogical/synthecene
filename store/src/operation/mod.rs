
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::metrics::MetricsSet;

use crate::table::TableStore;
use crate::result::CalicoResult;

pub mod append;

// move to command pattern
// https://rust-unofficial.github.io/patterns/patterns/behavioural/command.html

#[async_trait]
pub trait Operation<T> {

    // todo: refactor this for distributed execution:
    // 1. get execution plan - returns an execution plan that can be posted by partition
    // 2. get write plan - returns a write plan for how data should be written to storage
    // 3. commit - performs the transaction log update
    // 4. abort - cleans up any partitially committed updates

    async fn execute(&mut self, table_store: Arc<TableStore>) -> CalicoResult<T>;

    /// Attempt to cleanup any remnants of a failed operation
    ///
    async fn abort(&mut self, table_store: Arc<TableStore>) -> CalicoResult<()>;

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



struct _CheckpointOperation {

}


struct _RepartitionOperation {

}


struct _SetColumnMetadataOperation {

}

struct _SetColumnGroupMetadataOperation {

}


