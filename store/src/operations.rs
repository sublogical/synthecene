
use arrow::record_batch::RecordBatch;

use crate::writer::write_batches;
use crate::{CalicoTable, CalicoSchema};
use crate::partition::{ split_batch };
use crate::result::{CalicoResult};

pub async fn append_operation(calico_table: &CalicoTable, 
                              schema: &CalicoSchema,
                              batch:&RecordBatch) -> CalicoResult<()> { 
        
    let split_batches = split_batch(calico_table, batch).await?;
    let object_paths = write_batches(calico_table, &split_batches).await?;

    // create a transaction
    // add these commits


    Ok(())
}
