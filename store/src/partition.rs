use arrow::array::{BinaryArray, ArrayRef, UInt64Array};
use arrow::compute::take;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::error::Result as ArrowResult;
use itertools::Itertools;
use log::info;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::result::CalicoResult;

pub type PartitionSpec = u64;
pub type ColumnRef = String;
pub type ColumnGroupRef = String;

// todo: move to protobuf or whatever
#[derive(Debug, Clone)]
pub struct ColumnConfig {
    pub column: ColumnRef,
    pub column_group: ColumnGroupRef
}

#[derive(Debug, Clone)]
pub struct ColumnGroupConfig {
    pub partition_key: u8,   // index within the key-space to use for partitioning
    pub partition_size: PartitionSpec, // number of partitions to use
}


#[derive(Debug, Clone)]
pub struct TableConfig {
    pub column_config:          HashMap<ColumnRef, ColumnConfig>,
    pub column_group_config:    HashMap<ColumnGroupRef, ColumnGroupConfig>,
}

#[derive(Debug, Clone)]
pub struct Tile {
    pub column_group: ColumnGroupRef,
    pub partition_num: PartitionSpec
}

// Maps the ID column from a record batch into an columns of partition indices for all records
fn calc_partitions(column_group_config: &ColumnGroupConfig, batch: &RecordBatch) -> CalicoResult<UInt64Array> {
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
fn partition_indices(column_group_config: &ColumnGroupConfig, batch: &RecordBatch) -> CalicoResult<Vec<(PartitionSpec,UInt64Array)>> {
    let partitions = calc_partitions(column_group_config, batch)?;
    let partitions = partitions.values();

    let mut output:Vec<(PartitionSpec, Vec<u64>)> = Vec::with_capacity(column_group_config.partition_size as usize);
    for partition_num in 0..column_group_config.partition_size {
        output.push((partition_num, Vec::new()));
    }

    for (index, partition_num) in partitions.iter().enumerate() {
        output[*partition_num as usize].1.push(index as u64);
    }

    Ok(output.iter()
        .map(|(partition_num, indices_vec)| (*partition_num, UInt64Array::from(indices_vec.clone())))
        .collect())
}

/// Calculates the partition ID for a record based on the key
fn key_to_partition(column_group_config: &ColumnGroupConfig, key: &[u8]) -> PartitionSpec {
    let mut s = DefaultHasher::new();
    let mut index = 0;
    let mut matches = 0;

    while matches < column_group_config.partition_key+1 && index < key.len() {
        let curr = key.get(index).unwrap();
        if *curr == b'\0' {
            matches += 1;
        } else {
            curr.hash(&mut s);
        }
        index += 1;
    }

    info!("hash= {:x}", s.finish());

    return (s.finish() % column_group_config.partition_size as u64).try_into().unwrap();
}

// Use Table configuration to determine the map of column groups to columns for the table
fn extract_column_groups(table_config: &TableConfig, batch: &RecordBatch) -> Vec<(ColumnGroupRef, Vec<usize>)> {
    batch.schema().fields().iter()
        .enumerate()
        .group_by(|(_, field)| column_to_colgroup(table_config, field.name()))
        .into_iter()
        .filter(|(key, _)| key.is_some())
        .map(|(key, group)| (key.unwrap(), group.map(|(column_index, _)| column_index).collect()))
        .collect::<Vec<(ColumnGroupRef, Vec<usize>)>>()
}

/// Use Table configuration to determine the column groups for a set of columns
fn column_to_colgroup(table_config: &TableConfig, column: &ColumnRef) -> Option<ColumnGroupRef> {
    let output = table_config.column_config.get(column)
        .map(|cc| cc.column_group.to_owned());
    
    info!("lookup column_group for {}: {:?}", column, output);

    output
}

pub const ID_INDEX:usize = 0;
pub const ID_FIELD:&str = "id";

pub fn split_batch(table_config: &TableConfig, batch: &RecordBatch) -> CalicoResult<Vec<(Tile, Arc<RecordBatch>)>> {
    let column_groups = extract_column_groups(table_config, batch);

    let mut output = Vec::new();

    for (column_group, column_indices) in column_groups.iter() {
        info!("column group: {} has indices {:?}", column_group, column_indices);
        let config = table_config.column_group_config.get(column_group).unwrap();
        let partition_indices = partition_indices(config, batch)?;

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

            let tile = Tile {
                column_group: column_group.clone(),
                partition_num: *partition_num
            };

            output.push((tile, cell_batch));
        }
    }

    Ok(output)
}
 
#[cfg(test)]
mod tests {
    use arrow::array::{ Float32Array, BinaryArray };
    use maplit::hashmap;
    
    use crate::partition::*;

    #[test]
    fn test_split_batch() {
        const FIELD_A:&str = "a";
        const FIELD_B:&str = "b";
        const FIELD_C:&str = "c";
        const FIELD_D:&str = "d";

        const COLGROUP_1:&str = "cg1";
        const COLGROUP_2:&str = "cg2";

        let table_config = TableConfig {
            column_config:  hashmap![
                FIELD_A.to_string() => ColumnConfig { column: FIELD_A.to_string(), column_group: COLGROUP_1.to_string() },
                FIELD_B.to_string() => ColumnConfig { column: FIELD_B.to_string(), column_group: COLGROUP_1.to_string() },
                FIELD_C.to_string() => ColumnConfig { column: FIELD_C.to_string(), column_group: COLGROUP_2.to_string() },
                FIELD_D.to_string() => ColumnConfig { column: FIELD_D.to_string(), column_group: COLGROUP_2.to_string() },
            ],
            column_group_config: hashmap! [
                COLGROUP_1.to_string() => ColumnGroupConfig { partition_key: 0, partition_size: 2 },
                COLGROUP_2.to_string() => ColumnGroupConfig { partition_key: 0, partition_size: 1 }
            ]
        };

        let batch = RecordBatch::try_from_iter([
            (ID_FIELD, Arc::new(BinaryArray::from_vec(vec![b"bird", b"bird\0one", b"bird\0two", b"cat", b"cat\0one", b"cat\0two"])) as _),
            (FIELD_A,  Arc::new(Float32Array::from_iter([1., 1.1, 1.2, 2., 2.1, 2.2])) as _),
            (FIELD_B,  Arc::new(Float32Array::from_iter([1., 1.1, 1.2, 2., 2.1, 2.2])) as _),
            (FIELD_C,  Arc::new(Float32Array::from_iter([1., 1.1, 1.2, 2., 2.1, 2.2])) as _),
            (FIELD_D,  Arc::new(Float32Array::from_iter([1., 1.1, 1.2, 2., 2.1, 2.2])) as _)
        ]).unwrap();

        let splits = split_batch(&table_config, &batch).unwrap();

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