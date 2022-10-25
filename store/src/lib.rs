pub mod protocol {
    // todo: not windows specific (gah)
    include!(concat!(env!("OUT_DIR"), "\\calico.protocol.rs"));
}

mod writer;

pub mod log;
pub mod object;
pub mod operations;
pub mod partition;
pub mod table;
// pub mod reader;

pub struct CalicoSchema {
}

pub mod result {
    use std::{error::Error, fmt::Display};

    use arrow::error::ArrowError;
    use parquet::errors::ParquetError;
    use prost::{DecodeError, EncodeError};
    use tokio::task::JoinError;
    use object_store::Error as ObjectStoreError;

    #[derive(Debug)]
    pub enum CalicoError {
        ArrowMessedUp(ArrowError),
        JoinFailed(JoinError),
        EncodeError(EncodeError),
        DecodeError(DecodeError),
        ObjectStoreError(ObjectStoreError),
        BranchNotFound(String),
        ParquetError(ParquetError),

        PartitionError(&'static str),
        TransactionLogAlreadyIntialized(String),
        TransactionLogNotPresent(String),
        UnknownColumn(String),
        UnknownColumnGroup(String),
    }

    impl Display for CalicoError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self)
        }
    }
    impl Error for CalicoError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            match *self {
                Self::ArrowMessedUp(ref e) => Some(e),
                Self::JoinFailed(ref e) => Some(e),
                Self::EncodeError(ref e) => Some(e),
                Self::DecodeError(ref e) => Some(e),
                Self::ObjectStoreError(ref e) => Some(e),
                Self::ParquetError(ref e) => Some(e),
                _ => None
            }
        }
    }

    impl From<ArrowError> for CalicoError {
        fn from(err: ArrowError) -> Self {
            CalicoError::ArrowMessedUp(err)
        }
    }    
    impl From<JoinError> for CalicoError {
        fn from(err: JoinError) -> Self {
            CalicoError::JoinFailed(err)
        }
    }    
    impl From<EncodeError> for CalicoError {
        fn from(err: EncodeError) -> Self {
            CalicoError::EncodeError(err)
        }
    }    
    impl From<DecodeError> for CalicoError {
        fn from(err: DecodeError) -> Self {
            CalicoError::DecodeError(err)
        }
    }    
    impl From<ParquetError> for CalicoError {
        fn from(err: ParquetError) -> Self {
            CalicoError::ParquetError(err)
        }
    }    
    impl From<ObjectStoreError> for CalicoError {
        fn from(err: ObjectStoreError) -> Self {
            CalicoError::ObjectStoreError(err)
        }
    }    

    pub type CalicoResult<T> = Result<T, CalicoError>;
}


#[cfg(test)]
pub mod test_util {
    use std::sync::Arc;

    use arrow::{record_batch::RecordBatch, array::{BinaryArray, Float32Array, Array}};
    use datafusion::datasource::object_store::{ObjectStoreRegistry, ObjectStoreUrl};
    use object_store::{local::LocalFileSystem, ObjectStore};
    use itertools::concat;

    use crate::{table::{TableStore, ID_FIELD}, protocol};

    pub const FIELD_A:&str = "a";
    pub const FIELD_B:&str = "b";
    pub const FIELD_C:&str = "c";
    pub const FIELD_D:&str = "d";

    pub const COLGROUP_1:&str = "cg1";
    pub const COLGROUP_2:&str = "cg2";

    pub async fn provision_table(path: &std::path::Path, column_groups: &Vec<&str>) -> TableStore {
        let registry = ObjectStoreRegistry::new();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(path).unwrap());
        registry.register_store("file", "temp", object_store.clone());
        let object_store_url = ObjectStoreUrl::parse("file://temp").unwrap();
        let mut table_store:TableStore = TableStore::new(object_store_url, object_store).await.unwrap();

        if column_groups.contains(&COLGROUP_1) {
            table_store.add_column_group(protocol::ColumnGroupMetadata { 
                column_group: COLGROUP_1.to_string(),
                partition_spec: Some(protocol::column_group_metadata::PartitionSpec::KeyHash(
                    protocol::KeyHashPartition { num_keys: 0, num_partitions: 2 })
                )}).await.unwrap();

            table_store.add_column(protocol::ColumnMetadata { column: FIELD_A.to_string(), column_group: COLGROUP_1.to_string() }).await.unwrap();
            table_store.add_column(protocol::ColumnMetadata { column: FIELD_B.to_string(), column_group: COLGROUP_1.to_string() }).await.unwrap();
        }

        if column_groups.contains(&COLGROUP_2) {        
            table_store.add_column_group(protocol::ColumnGroupMetadata { 
                column_group: COLGROUP_2.to_string(),
                partition_spec: Some(protocol::column_group_metadata::PartitionSpec::KeyHash(
                    protocol::KeyHashPartition { num_keys: 0, num_partitions: 1 })
                )}).await.unwrap();

            table_store.add_column(protocol::ColumnMetadata { column: FIELD_C.to_string(), column_group: COLGROUP_2.to_string() }).await.unwrap();
            table_store.add_column(protocol::ColumnMetadata { column: FIELD_D.to_string(), column_group: COLGROUP_2.to_string() }).await.unwrap();
        }

        table_store
    }

    pub fn make_data(num_records: u64, start_id: u64, column_groups: &Vec<&str>) -> RecordBatch {
        let id_col = (0..num_records).into_iter()
            .map(|idx| Some(format!("{:08}", idx+start_id).as_bytes().to_owned()))
            .collect::<Vec<_>>();
        let id = Arc::new(BinaryArray::from_iter(id_col)) as _;

        let val_col = (0..num_records).into_iter()
            .map(|idx| 1.3 * idx as f32)
            .collect::<Vec<f32>>();
        let val:Arc<dyn Array> = Arc::new(Float32Array::from_iter(val_col)) as _;

        let mut cols = vec![ (ID_FIELD, id)];

        if column_groups.contains(&COLGROUP_1) {
            cols.append(&mut vec![ (FIELD_A, val.clone()), (FIELD_B, val.clone()) ]);
        }

        if column_groups.contains(&COLGROUP_1) {
            cols.append(&mut vec![ (FIELD_C, val.clone()), (FIELD_D, val.clone()) ]);
        }

        RecordBatch::try_from_iter(cols).unwrap()
    }

}

