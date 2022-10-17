
use arrow::record_batch::RecordBatch;

use crate::log::{MAINLINE};
use crate::protocol;
use crate::writer::write_batches;
use crate::partition::{ split_batch };
use crate::result::{CalicoResult};
use crate::table::CalicoTable;

pub async fn append_operation(calico_table: &CalicoTable, 
                              batch:&RecordBatch) -> CalicoResult<Vec<protocol::Commit>> { 
    
    let split_batches = split_batch(calico_table, batch).await?;
    let object_paths = write_batches(calico_table, &split_batches).await?;

    let cols = vec!["a".to_string()];
    let col_expr = vec![("a".to_string(), "$new".to_string())];
    let mut commits = vec![];

    for (tile, file) in object_paths {
        let log = calico_table.transaction_log_for(&tile).await?;
        let head_id = log.head_id_mainline().await?;
        
        // todo: proper timestamp
        let timestamp = 0;

        let commit = log.create_commit(
            &head_id.to_vec(), 
            None, 
            None, 
            None, 
            timestamp, 
            cols.to_vec(), 
            col_expr.to_vec(), 
            vec![file]).await?;
    
        let _new_head = log.fast_forward(MAINLINE, &commit.commit_id).await.unwrap();

        commits.push(commit.commit);
    }

    Ok(commits)
}



#[cfg(test)]
mod tests {
    use core::num;
    use std::sync::Arc;

    use arrow::{array::{ Float32Array, BinaryArray, Array }, record_batch::RecordBatch};
    use tempfile::tempdir;
    
    use crate::{partition::*, table::CalicoTable, protocol, operations::append_operation};

    const FIELD_A:&str = "a";
    const FIELD_B:&str = "b";
    const FIELD_C:&str = "c";
    const FIELD_D:&str = "d";

    const COLGROUP_1:&str = "cg1";
    const COLGROUP_2:&str = "cg2";

    #[tokio::test]
    async fn test_append() {
        let temp = tempdir().unwrap();
        let table = provision_table(temp.path()).await;

        append_operation(&table, &make_data(10,0)).await.unwrap();
        append_operation(&table, &make_data(10,10)).await.unwrap();
        append_operation(&table, &make_data(10, 20)).await.unwrap();
        
        let log = table.default_transaction_log().await.unwrap();
        let head = log.head_mainline().await.unwrap();
        let history = head.history(100).await.unwrap();
        assert_eq!(history.len(), 9);

        // make sure the files are in the object store
    }

    fn make_data(num_records: u64, start_id: u64) -> RecordBatch {
        let id_col = (0..num_records).into_iter()
            .map(|idx| Some(format!("{:08}", idx+start_id).as_bytes().to_owned()))
            .collect::<Vec<_>>();
        let id = Arc::new(BinaryArray::from_iter(id_col)) as _;

        let val_col = (0..num_records).into_iter()
            .map(|idx| 1.3 * idx as f32)
            .collect::<Vec<f32>>();
        let val:Arc<dyn Array> = Arc::new(Float32Array::from_iter(val_col)) as _;

        RecordBatch::try_from_iter([
            (ID_FIELD, id),
            (FIELD_A, val.clone()),
            (FIELD_B, val.clone()),
            (FIELD_C, val.clone()),
            (FIELD_D, val.clone())
        ]).unwrap()
    }

    async fn provision_table(path: &std::path::Path) -> CalicoTable {
        let mut table:CalicoTable = CalicoTable::from_local(path).await.unwrap();

        table.add_column_group(protocol::ColumnGroupMetadata { 
            column_group: COLGROUP_1.to_string(),
            partition_spec: Some(protocol::column_group_metadata::PartitionSpec::KeyHash(
                protocol::KeyHashPartition { num_keys: 0, num_partitions: 2 })
            )}).await.unwrap();

        table.add_column_group(protocol::ColumnGroupMetadata { 
            column_group: COLGROUP_2.to_string(),
            partition_spec: Some(protocol::column_group_metadata::PartitionSpec::KeyHash(
                protocol::KeyHashPartition { num_keys: 0, num_partitions: 1 })
            )}).await.unwrap();

        table.add_column(protocol::ColumnMetadata { column: FIELD_A.to_string(), column_group: COLGROUP_1.to_string() }).await.unwrap();
        table.add_column(protocol::ColumnMetadata { column: FIELD_B.to_string(), column_group: COLGROUP_1.to_string() }).await.unwrap();
        table.add_column(protocol::ColumnMetadata { column: FIELD_C.to_string(), column_group: COLGROUP_2.to_string() }).await.unwrap();
        table.add_column(protocol::ColumnMetadata { column: FIELD_D.to_string(), column_group: COLGROUP_2.to_string() }).await.unwrap();

        table
    }

}