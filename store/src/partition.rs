use arrow::array::*;
use arrow::compute::take;
use arrow::datatypes::{Schema, DataType };
use arrow::record_batch::RecordBatch;
use arrow::error::Result as ArrowResult;
use datafusion::physical_plan::SendableRecordBatchStream;
use log::info;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use calico_shared::result::{CalicoResult, CalicoError};
use crate::protocol;
use crate::table::TableStore;

// Maps the ID column from a record batch into an columns of partition indices for all records
fn calc_partitions(column_group_config: &protocol::ColumnGroupMetadata, batch: &RecordBatch) -> CalicoResult<Int32Array> {

    use protocol::column_group_metadata::PartitionSpec;

    match &column_group_config.partition_spec {
        Some(PartitionSpec::KeyHash(hash)) => calc_hash_partitions(&hash, batch),
        None => panic!("no partitioning scheme"),
    }

}

fn calc_hash_partitions(key_hash: &protocol::KeyHashPartition, batch: &RecordBatch) -> CalicoResult<Int32Array> {

    // This allows us to create an iterator over a dynamic Hash type
    // (e.g. string, i32), which isn't possible with the default Hash trait
    // because it uses a generic type, which messes up the vtable. We 
    // get around this by creating a specialized trait for a non-generic Hasher

    struct Hashem<T> where T: Hash {
        value:T
    }
    trait Hashit {
        fn hash(&self, state: &mut DefaultHasher);
    }
    impl <T> Hashit for Hashem<T>  where T: Hash {
        fn hash(&self, state: &mut DefaultHasher) {
            self.value.hash(state);
        }
    }

    macro_rules! iter_generic_array {
        ($col:expr, $T:ty) => {
            {
                let indices: Box<dyn Iterator<Item = Option<Box<dyn Hashit>>>> = Box::new($col
                    .as_any()
                    .downcast_ref::<$T>()
                    .unwrap()
                    .iter()
                    .map(|item|
                        item.map(|inner| {
                            let boxed: Box<dyn Hashit> = Box::new(Hashem { value : inner });
                            boxed
                        })
                    )
                );
                indices
            }
        }
    }

    let columns_for_partition = vec![0];

    let column_hash_iterators = columns_for_partition.iter().map(|column_number| {
        let partition_column = batch.column(*column_number);
        let part_hash_iterator = match partition_column.data_type() {
            DataType::Int8 => iter_generic_array!(partition_column, Int8Array),
            DataType::Int16 => iter_generic_array!(partition_column, Int16Array),
            DataType::Int32 => iter_generic_array!(partition_column, Int32Array),
            DataType::Int64 => iter_generic_array!(partition_column, Int64Array),
            DataType::UInt8 => iter_generic_array!(partition_column, Int8Array),
            DataType::UInt16 => iter_generic_array!(partition_column, UInt16Array),
            DataType::UInt32 => iter_generic_array!(partition_column, UInt32Array),
            DataType::UInt64 => iter_generic_array!(partition_column, UInt64Array),
            DataType::Utf8 => iter_generic_array!(partition_column, StringArray),
            _ => panic!("Partitioning not supported on that type")
        };

        part_hash_iterator
    }).collect();

    // todo: move this into a general purpose location

    struct Multizip<T>(Vec<T>);

    impl <T> Iterator for Multizip<T>
    where
        T: Iterator,
    {
        type Item = Vec<T::Item>;

        fn next(&mut self) -> Option<Self::Item> {
            self.0.iter_mut().map(Iterator::next).collect()
        }
    }
    let zipped_columns = Multizip(column_hash_iterators);

    let partitions:Int32Array = zipped_columns.map(|hashables| {
        let mut hasher = DefaultHasher::new();


        for hashable in hashables {
            match hashable {
                Some(value) => value.hash(&mut hasher),
                None => {}
            }
        }

        let part:i32 = (hasher.finish() % key_hash.num_partitions as u64).try_into().unwrap();
        part
    }).collect();

    Ok(partitions)
}

// Maps the ID column from a record batch into a vector containing a vector of indices for each partition
fn partition_indices(column_group_config: &protocol::ColumnGroupMetadata, batch: &RecordBatch) -> CalicoResult<Vec<(u64,UInt64Array)>> {
    let partitions = calc_partitions(column_group_config, batch)?;
    let max_partition:i32 = arrow::compute::max(&partitions).ok_or(CalicoError::PartitionError("unexpected error computing max partition"))?;
    let partitions = partitions.values();
    
    let mut output:Vec<(u64, Vec<u64>)> = Vec::with_capacity((max_partition + 1) as usize);
    for partition_num in 0..=max_partition {
        output.push((partition_num.try_into().unwrap(), Vec::new()));
    }

    for (index, partition_num) in partitions.iter().enumerate() {
        output[*partition_num as usize].1.push(index as u64);
    }

    Ok(output.iter()
        .map(|(partition_num, indices_vec)| (*partition_num, UInt64Array::from(indices_vec.clone())))
        .collect())
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
                .filter(|(field_index, _)| column_indices.contains(field_index))
                .map(|(_, field)| field.to_owned())
                .collect()));
            
        for (partition_num, row_indices) in partition_indices.iter() {
            let cell_batch = Arc::new(RecordBatch::try_new(
                column_group_schema.clone(),
                batch
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(column_index, _)| column_indices.contains(column_index))
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
    use datafusion::assert_batches_sorted_eq;
    use tempfile::tempdir;
    
    use crate::partition::*;
    use crate::test_util::*;

    #[tokio::test]
    async fn test_split_batch() {
        let col_groups = vec![COLGROUP_PARTITIONED, COLGROUP_UNPARTITIONED];

        let temp = tempdir().unwrap();
        let ctx = provision_ctx(temp.path());
        let table_store = Arc::new(provision_store(&ctx, &col_groups).await);

        let batch = build_table(
            &vec![
                (ID_FIELD, i32_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                (FIELD_A, i32_col(&vec![11, 12, 13, 14, 15, 16, 17, 18, 19])),
                (FIELD_B, i32_col(&vec![21, 22, 23, 24, 25, 26, 27, 28, 29])),
                (FIELD_C, i32_col(&vec![31, 32, 33, 34, 35, 36, 37, 38, 39])),
                (FIELD_D, i32_col(&vec![41, 42, 43, 44, 45, 46, 47, 48, 49])),
            ]
        );

        let splits = split_batch(&table_store, &batch).await.unwrap();

        assert_eq!(splits.len(), 3);
        assert_eq!(splits[0].0.partition_num, 0);
        assert_eq!(splits[0].0.column_group, COLGROUP_1);

        let expected_0 = vec![
            "+----+----+----+",
            "| id | a  | b  |",
            "+----+----+----+",
            "| 0  | 11 | 21 |",
            "| 1  | 12 | 22 |",
            "| 3  | 14 | 24 |",
            "| 4  | 15 | 25 |",
            "| 7  | 18 | 28 |",
            "+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected_0, &[(*splits[0].1).clone()]);

        assert_eq!(splits[1].0.partition_num, 1);
        assert_eq!(splits[1].0.column_group, COLGROUP_1);

        let expected_1 = vec![
            "+----+----+----+",
            "| id | a  | b  |",
            "+----+----+----+",
            "| 2  | 13 | 23 |",
            "| 5  | 16 | 26 |",
            "| 6  | 17 | 27 |",
            "| 8  | 19 | 29 |",
            "+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected_1, &[(*splits[1].1).clone()]);

        assert_eq!(splits[2].0.partition_num, 0);
        assert_eq!(splits[2].0.column_group, COLGROUP_2);

        let expected_2 = vec![
            "+----+----+----+",
            "| id | c  | d  |",
            "+----+----+----+",
            "| 0  | 31 | 41 |",
            "| 1  | 32 | 42 |",
            "| 2  | 33 | 43 |",
            "| 3  | 34 | 44 |",
            "| 4  | 35 | 45 |",
            "| 5  | 36 | 46 |",
            "| 6  | 37 | 47 |",
            "| 7  | 38 | 48 |",
            "| 8  | 39 | 49 |",
            "+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected_2, &[(*splits[2].1).clone()]);
    }

    #[tokio::test]
    async fn hash_partition_numerics() {
        let col_groups = vec![COLGROUP_PARTITIONED, COLGROUP_UNPARTITIONED];

        let temp = tempdir().unwrap();
        let ctx = provision_ctx(temp.path());
        let table_store = Arc::new(provision_store(&ctx, &col_groups).await);

        let batches = vec![
            build_table(&vec![
                (ID_FIELD, i16_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                (FIELD_A, i32_col(&vec![11, 12, 13, 14, 15, 16, 17, 18, 19])),
                (FIELD_B, i32_col(&vec![21, 22, 23, 24, 25, 26, 27, 28, 29])),
            ]),
            build_table(&vec![
                (ID_FIELD, i32_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                (FIELD_A, i32_col(&vec![11, 12, 13, 14, 15, 16, 17, 18, 19])),
                (FIELD_B, i32_col(&vec![21, 22, 23, 24, 25, 26, 27, 28, 29])),
            ]),
            build_table(&vec![
                (ID_FIELD, i64_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                (FIELD_A, i32_col(&vec![11, 12, 13, 14, 15, 16, 17, 18, 19])),
                (FIELD_B, i32_col(&vec![21, 22, 23, 24, 25, 26, 27, 28, 29])),
            ]),
            build_table(&vec![
                (ID_FIELD, u16_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                (FIELD_A, i32_col(&vec![11, 12, 13, 14, 15, 16, 17, 18, 19])),
                (FIELD_B, i32_col(&vec![21, 22, 23, 24, 25, 26, 27, 28, 29])),
            ]),
            build_table(&vec![
                (ID_FIELD, u32_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                (FIELD_A, i32_col(&vec![11, 12, 13, 14, 15, 16, 17, 18, 19])),
                (FIELD_B, i32_col(&vec![21, 22, 23, 24, 25, 26, 27, 28, 29])),
            ]),
            build_table(&vec![
                (ID_FIELD, u64_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                (FIELD_A, i32_col(&vec![11, 12, 13, 14, 15, 16, 17, 18, 19])),
                (FIELD_B, i32_col(&vec![21, 22, 23, 24, 25, 26, 27, 28, 29])),
            ]),
        ];

        for batch in batches {
            let splits = split_batch(&table_store, &batch).await.unwrap();

            assert_eq!(splits.len(), 2);
            assert_eq!(splits[0].0.partition_num, 0);
            assert_eq!(splits[0].0.column_group, COLGROUP_1);
            assert_eq!(splits[1].0.partition_num, 1);
            assert_eq!(splits[1].0.column_group, COLGROUP_1);

            assert_eq!(splits[0].1.num_columns(), 3);
            assert_eq!(splits[1].1.num_columns(), 3);

            assert!(splits[0].1.num_rows() > 0);
            assert!(splits[1].1.num_rows() > 0);

            let merged_splits = vec![
                (*splits[0].1).clone(),
                (*splits[1].1).clone()
            ];

            let expected = vec![
                "+----+----+----+",
                "| id | a  | b  |",
                "+----+----+----+",
                "| 0  | 11 | 21 |",
                "| 1  | 12 | 22 |",
                "| 2  | 13 | 23 |",
                "| 3  | 14 | 24 |",
                "| 4  | 15 | 25 |",
                "| 5  | 16 | 26 |",
                "| 6  | 17 | 27 |",
                "| 7  | 18 | 28 |",
                "| 8  | 19 | 29 |",
                "+----+----+----+",
            ];
            assert_batches_sorted_eq!(expected, &merged_splits);    
        }
    }

    #[tokio::test]
    async fn hash_partition_string() {
        let col_groups = vec![COLGROUP_PARTITIONED, COLGROUP_UNPARTITIONED];

        let temp = tempdir().unwrap();
        let ctx = provision_ctx(temp.path());
        let table_store = Arc::new(provision_store(&ctx, &col_groups).await);

        let batch = 
            build_table(&vec![
                (ID_FIELD, str_col(&vec!["a", "b", "c", "d", "e", "f", "g", "h", "i"])),
                (FIELD_A, i32_col(&vec![11, 12, 13, 14, 15, 16, 17, 18, 19])),
                (FIELD_B, i32_col(&vec![21, 22, 23, 24, 25, 26, 27, 28, 29])),
            ]);

        let splits = split_batch(&table_store, &batch).await.unwrap();

        assert_eq!(splits.len(), 2);
        assert_eq!(splits[0].0.partition_num, 0);
        assert_eq!(splits[0].0.column_group, COLGROUP_1);

        let expected_0 = vec![
            "+----+----+----+",
            "| id | a  | b  |",
            "+----+----+----+",
            "| b  | 12 | 22 |",
            "| c  | 13 | 23 |",
            "| e  | 15 | 25 |",
            "| g  | 17 | 27 |",
            "| i  | 19 | 29 |",
            "+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected_0, &[(*splits[0].1).clone()]);    

        assert_eq!(splits[1].0.partition_num, 1);
        assert_eq!(splits[1].0.column_group, COLGROUP_1);

        let expected_1 = vec![
            "+----+----+----+",
            "| id | a  | b  |",
            "+----+----+----+",
            "| a  | 11 | 21 |",
            "| d  | 14 | 24 |",
            "| f  | 16 | 26 |",
            "| h  | 18 | 28 |",
            "+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected_1, &[(*splits[1].1).clone()]);    

    }


}