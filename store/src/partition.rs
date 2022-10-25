use arrow::array::{BinaryArray, ArrayRef, UInt64Array};
use arrow::compute::take;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::error::Result as ArrowResult;
use datafusion::physical_plan::SendableRecordBatchStream;
use log::info;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::result::{CalicoResult, CalicoError};
use crate::protocol;
use crate::table::{TableStore, ID_INDEX};

// Maps the ID column from a record batch into an columns of partition indices for all records
fn calc_partitions(column_group_config: &protocol::ColumnGroupMetadata, batch: &RecordBatch) -> CalicoResult<UInt64Array> {
    let indices: UInt64Array = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .iter()
        .map(|value| value.map(|value| key_to_partition(column_group_config, value)))
        .collect();

    Ok(indices)
}

// Maps the ID column from a record batch into a vector containing a vector of indices for each partition
fn partition_indices(column_group_config: &protocol::ColumnGroupMetadata, batch: &RecordBatch) -> CalicoResult<Vec<(u64,UInt64Array)>> {
    let partitions = calc_partitions(column_group_config, batch)?;
    let max_partition:u64 = arrow::compute::max(&partitions).ok_or(CalicoError::PartitionError("unexpected error computing max partition"))?;
    let partitions = partitions.values();
    
    let mut output:Vec<(u64, Vec<u64>)> = Vec::with_capacity((max_partition + 1) as usize);
    for partition_num in 0..=max_partition {
        output.push((partition_num, Vec::new()));
    }

    for (index, partition_num) in partitions.iter().enumerate() {
        output[*partition_num as usize].1.push(index as u64);
    }

    Ok(output.iter()
        .map(|(partition_num, indices_vec)| (*partition_num, UInt64Array::from(indices_vec.clone())))
        .collect())
}

fn key_to_partition(column_group_config: &protocol::ColumnGroupMetadata, key: &[u8]) -> u64 {
    use protocol::column_group_metadata::PartitionSpec;

    match &column_group_config.partition_spec {
        Some(PartitionSpec::KeyHash(hash)) => key_hash_partition(&hash, key),
        None => 0,
    }
}

// Calculates the partition ID for a record based on the key using hashing
fn key_hash_partition(key_hash: &protocol::KeyHashPartition, key: &[u8]) -> u64 {
    let mut s = DefaultHasher::new();
    let mut index = 0;
    let mut matches = 0;

    while matches < key_hash.num_keys+1 && index < key.len() {
        let curr = key.get(index).unwrap();
        if *curr == b'\0' {
            matches += 1;
        } else {
            curr.hash(&mut s);
        }
        index += 1;
    }

    return (s.finish() % key_hash.num_partitions as u64).try_into().unwrap();
}

pub async fn split_batch(table_store: &TableStore, batch: &RecordBatch) -> CalicoResult<Vec<(protocol::Tile, Arc<RecordBatch>)>> {
    let column_groups = table_store.extract_column_groups(batch.schema()).await?;

    let mut output = Vec::new();

    for (column_group, column_indices) in column_groups.iter() {
        info!("column group: {} has indices {:?}", column_group, column_indices);
        let config = table_store.column_group_meta(column_group).await?;
        let partition_indices = partition_indices(&config, batch)?;

        let column_group_schema = Arc::new(Schema::new(
            batch.schema().fields()
                .iter()
                .enumerate()
                .filter(|(field_index, _)| *field_index == ID_INDEX || column_indices.contains(field_index))
                .map(|(_, field)| field.to_owned())
                .collect()));
            
        for (partition_num, row_indices) in partition_indices.iter() {
            let cell_batch = Arc::new(RecordBatch::try_new(
                column_group_schema.clone(),
                batch
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(column_index, _)| *column_index == ID_INDEX || column_indices.contains(column_index))
                    .map(|(_, column)| take(column.as_ref(), row_indices, None))
                    .collect::<ArrowResult<Vec<ArrayRef>>>()?
            )?);

            let tile = protocol::Tile {
                column_group: column_group.to_string(),
                partition_num: *partition_num
            };

            output.push((tile, cell_batch));
        }
    }

    Ok(output)
}
 
fn _split_stream(_table_store: &TableStore, 
                _stream: SendableRecordBatchStream) -> 
                CalicoResult<HashMap<protocol::Tile, SendableRecordBatchStream>> {
    todo!("perform same partitioning algorithm but on streams");
}


#[cfg(test)]
mod tests {
    use arrow::array::{ Float32Array, BinaryArray };
    use datafusion::{datasource::object_store::{ObjectStoreRegistry, ObjectStoreUrl}};
    use object_store::{ObjectStore, local::LocalFileSystem};
    use tempfile::tempdir;
    
    use crate::{partition::*, table::ID_FIELD};

    #[tokio::test]
    async fn test_split_batch() {
        const FIELD_A:&str = "a";
        const FIELD_B:&str = "b";
        const FIELD_C:&str = "c";
        const FIELD_D:&str = "d";

        const COLGROUP_1:&str = "cg1";
        const COLGROUP_2:&str = "cg2";

        let temp = tempdir().unwrap();

        let registry = ObjectStoreRegistry::new();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(temp.path()).unwrap());
        registry.register_store("file", "temp", object_store.clone());
        let object_store_url = ObjectStoreUrl::parse("file://temp").unwrap();
        let mut table_store:TableStore = TableStore::new(object_store_url, object_store).await.unwrap();

        table_store.add_column_group(protocol::ColumnGroupMetadata { 
            column_group: COLGROUP_1.to_string(),
            partition_spec: Some(protocol::column_group_metadata::PartitionSpec::KeyHash(
                protocol::KeyHashPartition { num_keys: 0, num_partitions: 2 })
            )}).await.unwrap();

        table_store.add_column_group(protocol::ColumnGroupMetadata { 
            column_group: COLGROUP_2.to_string(),
            partition_spec: Some(protocol::column_group_metadata::PartitionSpec::KeyHash(
                protocol::KeyHashPartition { num_keys: 0, num_partitions: 1 })
            )}).await.unwrap();

        table_store.add_column(protocol::ColumnMetadata { column: FIELD_A.to_string(), column_group: COLGROUP_1.to_string() }).await.unwrap();
        table_store.add_column(protocol::ColumnMetadata { column: FIELD_A.to_string(), column_group: COLGROUP_1.to_string() }).await.unwrap();
        table_store.add_column(protocol::ColumnMetadata { column: FIELD_B.to_string(), column_group: COLGROUP_1.to_string() }).await.unwrap();
        table_store.add_column(protocol::ColumnMetadata { column: FIELD_C.to_string(), column_group: COLGROUP_2.to_string() }).await.unwrap();
        table_store.add_column(protocol::ColumnMetadata { column: FIELD_D.to_string(), column_group: COLGROUP_2.to_string() }).await.unwrap();

        let batch = RecordBatch::try_from_iter([
            (ID_FIELD, Arc::new(BinaryArray::from_vec(vec![b"bird", b"bird\0one", b"bird\0two", b"cat", b"cat\0one", b"cat\0two"])) as _),
            (FIELD_A,  Arc::new(Float32Array::from_iter([1., 1.1, 1.2, 2., 2.1, 2.2])) as _),
            (FIELD_B,  Arc::new(Float32Array::from_iter([1., 1.1, 1.2, 2., 2.1, 2.2])) as _),
            (FIELD_C,  Arc::new(Float32Array::from_iter([1., 1.1, 1.2, 2., 2.1, 2.2])) as _),
            (FIELD_D,  Arc::new(Float32Array::from_iter([1., 1.1, 1.2, 2., 2.1, 2.2])) as _)
        ]).unwrap();

        let splits = split_batch(&table_store, &batch).await.unwrap();

        assert_eq!(splits.len(), 3);
        assert_eq!(splits[0].0.partition_num, 0);
        assert_eq!(splits[0].0.column_group, COLGROUP_1);
        assert_eq!(splits[0].1.num_rows(), 3);
        assert_eq!(splits[0].1.num_columns(), 3);

        assert_eq!(splits[1].0.partition_num, 1);
        assert_eq!(splits[1].0.column_group, COLGROUP_1);
        assert_eq!(splits[1].1.num_rows(), 3);
        assert_eq!(splits[1].1.num_columns(), 3);

        assert_eq!(splits[2].0.partition_num, 0);
        assert_eq!(splits[2].0.column_group, COLGROUP_2);
        assert_eq!(splits[2].1.num_rows(), 6);
        assert_eq!(splits[2].1.num_columns(), 3);
    }

}