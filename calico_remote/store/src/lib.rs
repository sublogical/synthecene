#[cfg(host_family = "windows")]
macro_rules! PATH_SEPARATOR {() => (
    r"\"
)}
#[cfg(not(host_family = "windows"))]
macro_rules! PATH_SEPARATOR {() => (
    r"/"
)}

pub mod protocol {
    include!(concat!(env!("OUT_DIR"), PATH_SEPARATOR!(), "calico.protocol.rs"));
}

mod writer;

pub mod blob;
pub mod datatypes;
pub mod log;
pub mod partition;
pub mod datafusion;
pub mod table;

pub mod test_util {
    use std::sync::Arc;

    use arrow::{record_batch::RecordBatch, array::*, datatypes::{Schema, DataType, Field}};
    use datafusion::{datasource::object_store::{ ObjectStoreUrl}, prelude::SessionContext};
    use object_store::{local::LocalFileSystem, ObjectStore};
    use itertools::Itertools;
    use rand::Rng;
    use rand::distributions::{Alphanumeric, DistString, Standard};

    use crate::{table::TableStore, protocol};

    pub const ID_INDEX:usize = 0;
    pub const ID_FIELD:&str = "id";
    
    pub const FIELD_A:&str = "a";
    pub const FIELD_B:&str = "b";
    pub const FIELD_C:&str = "c";
    pub const FIELD_D:&str = "d";

    pub const COLGROUP_1:&str = "cg1";
    pub const COLGROUP_PARTITIONED:&str = COLGROUP_1;
    pub const COLGROUP_2:&str = "cg2";
    pub const COLGROUP_UNPARTITIONED:&str = COLGROUP_2;

    pub(crate) fn provision_ctx(path: &std::path::Path) -> SessionContext {
        let ctx = SessionContext::new();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(path).unwrap());

        ctx.state
            .try_write().unwrap().runtime_env.register_object_store("file", "temp", object_store);
            
        
        ctx
    }

    pub(crate) async fn provision_store(ctx: &SessionContext, column_groups: &Vec<&str>) -> TableStore {
        let object_store_url = ObjectStoreUrl::parse("file://temp").unwrap();
        let object_store = ctx.state.read().runtime_env.object_store(&object_store_url).unwrap();
        let mut table_store:TableStore = TableStore::new(object_store_url, object_store).await.unwrap();

        if column_groups.contains(&COLGROUP_1) {
            table_store.add_column_group(protocol::ColumnGroupMetadata { 
                column_group: COLGROUP_1.to_string(),
                id_columns: vec![ID_FIELD.to_string()],
                partition_spec: Some(protocol::column_group_metadata::PartitionSpec::KeyHash(
                    protocol::KeyHashPartition { partition_keys: vec![ID_FIELD.to_string()], num_partitions: 2 })
                )}).await.unwrap();

            table_store.add_column(protocol::ColumnMetadata { column: FIELD_A.to_string(), column_group: COLGROUP_1.to_string() }).await.unwrap();
            table_store.add_column(protocol::ColumnMetadata { column: FIELD_B.to_string(), column_group: COLGROUP_1.to_string() }).await.unwrap();
        }

        if column_groups.contains(&COLGROUP_2) {        
            table_store.add_column_group(protocol::ColumnGroupMetadata { 
                column_group: COLGROUP_2.to_string(),
                id_columns: vec![ID_FIELD.to_string()],
                partition_spec: Some(protocol::column_group_metadata::PartitionSpec::KeyHash(
                    protocol::KeyHashPartition { partition_keys: vec![ID_FIELD.to_string()], num_partitions: 1 })
                )}).await.unwrap();

            table_store.add_column(protocol::ColumnMetadata { column: FIELD_C.to_string(), column_group: COLGROUP_2.to_string() }).await.unwrap();
            table_store.add_column(protocol::ColumnMetadata { column: FIELD_D.to_string(), column_group: COLGROUP_2.to_string() }).await.unwrap();
        }

        table_store
    }

    pub fn make_schema(columns: &Vec<&str>) -> Arc<Schema> {
        let mut fields = vec![ Field::new(ID_FIELD, DataType::Int32, false) ];
        let mut additional = columns.iter().map(|name| Field::new(name, DataType::Int32, false)).collect::<Vec<Field>>();

        fields.append(&mut additional);

        Arc::new(Schema::new(fields))
    }

    pub fn make_data(num_records: i32, 
                     start_id: i32,
                     data_offset: i32,
                     column_groups: &Vec<&str>) -> RecordBatch {
        let id_col = (0..num_records).into_iter()
            .map(|idx| Some(idx+start_id))
            .collect::<Vec<_>>();
        let id = Arc::new(Int32Array::from_iter(id_col)) as _;

        let val_col = (0..num_records).into_iter()
            .map(|idx| data_offset + 2 * idx)
            .collect::<Vec<i32>>();
        let val:Arc<dyn Array> = Arc::new(Int32Array::from_iter(val_col)) as _;

        let mut cols = vec![ (ID_FIELD, id)];

        if column_groups.contains(&COLGROUP_1) {
            cols.append(&mut vec![ (FIELD_A, val.clone()), (FIELD_B, val.clone()) ]);
        }

        if column_groups.contains(&COLGROUP_2) {
            cols.append(&mut vec![ (FIELD_C, val.clone()), (FIELD_D, val.clone()) ]);
        }

        RecordBatch::try_from_iter(cols).unwrap()
    }

    /// returns a table with 3 columns of i32 in memory
    pub fn build_table_i32(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
            Field::new(b.0, DataType::Int32, false),
            Field::new(c.0, DataType::Int32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap()
    }

    /// returns a table with 5 columns of i32 in memory
    pub fn build_table_i32_5(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
        d: (&str, &Vec<i32>),
        e: (&str, &Vec<i32>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
            Field::new(b.0, DataType::Int32, false),
            Field::new(c.0, DataType::Int32, false),
            Field::new(d.0, DataType::Int32, false),
            Field::new(e.0, DataType::Int32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
                Arc::new(Int32Array::from(d.1.clone())),
                Arc::new(Int32Array::from(e.1.clone())),
            ],
        )
        .unwrap()
    }

    pub fn i16_col(data: &Vec<i16>) -> ArrayRef {
        Arc::new(Int16Array::from(data.clone()))
    }

    pub fn i32_col(data: &Vec<i32>) -> ArrayRef {
        Arc::new(Int32Array::from(data.clone()))
    }

    pub fn i64_col(data: &Vec<i64>) -> ArrayRef {
        Arc::new(Int64Array::from(data.clone()))
    }

    pub fn u16_col(data: &Vec<u16>) -> ArrayRef {
        Arc::new(UInt16Array::from(data.clone()))
    }

    pub fn u32_col(data: &Vec<u32>) -> ArrayRef {
        Arc::new(UInt32Array::from(data.clone()))
    }

    pub fn u64_col(data: &Vec<u64>) -> ArrayRef {
        Arc::new(UInt64Array::from(data.clone()))
    }

    pub fn str_col(data: &Vec<&str>) -> ArrayRef {
        Arc::new(StringArray::from(data.clone()))
    }
    
    /// returns a table with 5 columns of i32 in memory
    pub fn build_table(cols: &Vec<(&str, ArrayRef)>) -> RecordBatch {
        let schema = Schema::new(cols.iter().map(|(name, col_data)| {
            Field::new(name, col_data.data_type().clone(), false)
        }).collect());


        let table_data:Vec<ArrayRef> = cols.iter().map(|(_, col_data)| {
            col_data.clone()
        }).collect();

        RecordBatch::try_new(Arc::new(schema),table_data).unwrap()
    }

    pub fn big_str_col(string_size:usize, num_strings:usize) -> Vec<String>{
        (1..num_strings).map(|_| {
            Alphanumeric.sample_string(&mut rand::thread_rng(), string_size)
        }).collect_vec()
    }
    
    pub fn big_col<T>(num:usize) -> Vec<T> 
        where Standard: rand::distributions::Distribution<T>
    {
        let mut rng = rand::thread_rng();
        (1..num).map(|_| {
            rng.gen()
        }).collect_vec()
    }    
}

