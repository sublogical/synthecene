
use arrow::record_batch::RecordBatch;

use crate::log::TransactionLog;
use crate::log_protocol::File;
use crate::writer::StorageConfig;
use crate::partition::{ TableConfig, split_batch, Tile };
use crate::object::{ObjectStore, write_batches};
use crate::result::{CalicoResult, CalicoError};

// todo: move to dedicated error/result classes
// todo: support async writes

fn object_to_addfile_action(cell: &Tile, path: &String) -> Action {
    let action = Action::default();
    action.action = Some(log_protocol::action::Action::Add(
        AddFile::default()
    ));

    action
}

pub async fn append_batch(
    storage_config: &StorageConfig, 
    table_config: &TableConfig,
    log: &TransactionLog,
    object_store: &ObjectStore,
    batch:&RecordBatch) -> CalicoResult<()> { 
        
    let split_batches = split_batch(table_config, batch)?;
    let object_paths = write_batches(object_store, split_batches).await?;

    log.with_new_transaction(|txn| {
        
        txn.commit(object_paths.iter().map(|(cell, path)| 
            object_to_addfile_action(cell, path)
            ).collect::<Vec<Action>>());

        Ok::<_,CalicoError>(())
    });


    // create a transaction
    // add these commits


    Ok(())
}

pub async fn merge_batch() -> CalicoResult<()> { 
    Ok(())
}
