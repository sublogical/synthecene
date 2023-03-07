use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::scalar::ScalarValue;
use uuid::Uuid;
use std::collections::HashMap;
use std::sync::Arc;
use arrow::datatypes::{SchemaRef as ArrowSchemaRef, Schema as ArrowSchema};
use itertools::Itertools;
use object_store::{ObjectStore, ObjectMeta};
use object_store::path::Path as ObjectStorePath;

use crate::datatypes::timestamp_to_datetime;
use crate::log::{TransactionLog, ReferencePoint, TableAction};
use crate::protocol;
use calico_shared::result::{CalicoResult, CalicoError};

// todo: convert this to calico-specific schema objects to allow for protobuf types & column metadata
type Schema = ArrowSchema;
type SchemaRef = ArrowSchemaRef;

pub struct TableStore {
    pub(crate) object_store_url: ObjectStoreUrl,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) log: Arc<TransactionLog>,

    column_group_config: HashMap<String, protocol::ColumnGroupMetadata>,
    column_config: HashMap<String, protocol::ColumnMetadata>,
}

type TableStoreRef = Arc<TableStore>;

pub struct Table {
    pub(crate) store: TableStoreRef,
    pub(crate) schema: SchemaRef,
    pub(crate) reference: ReferencePoint,
}

impl From<&protocol::File> for ObjectMeta {
    fn from(file: &protocol::File) -> Self {
        let last_modified = timestamp_to_datetime(file.update_time);
        ObjectMeta {
            location: ObjectStorePath::from(file.file_path.clone()),
            last_modified,
            size: file.file_size as usize,
        }
    }
}

pub(crate) async fn make_schema_for_action(object_store:&Arc<dyn ObjectStore>, action:&TableAction, column_group:&str) -> Arc<Schema> {
    let tile_files = match action {
        TableAction::Checkpoint(checkpoint) => &checkpoint.tile_files,
        TableAction::Commit(commit) => &commit.tile_files
    };

    let file = tile_files.iter()
        .filter_map(|tile_file|{
            let tile = tile_file.tile.as_ref().expect("Should never have a tile_file with no tile");
            
            if tile.column_group != column_group {
                None
            } else {
                Some(tile_file.file[0].clone())
            }
        })
        .next().expect("must have at least one file");

    let meta:ObjectMeta = (&file).into();
    
    let format = ParquetFormat::default();
    let schema = format.infer_schema(object_store, &[meta]).await.unwrap();

    schema
}

pub const OBJECT_PATH: &'static str = "objects";

impl TableStore {
    pub async fn new(object_store_url:ObjectStoreUrl, 
                     object_store:Arc<dyn ObjectStore>) -> CalicoResult<TableStore> {

        let log = Arc::new(TransactionLog::init(object_store.clone()).await?);

        let table = TableStore {
            object_store_url,
            object_store,
            log,
            column_config: HashMap::new(),
            column_group_config: HashMap::new()
        };

        Ok(table)
    }

    pub async fn load() -> CalicoResult<TableStore> {
        
        // load object_store for table metadata
        // for columns requested, get column-groups
        // load column-group metadata: partitioning, keyspace, all columns
        // lazy load object_stores & transaction logs for all tiles
        todo!()
    }

    pub async fn data_store_for(&self, _tile: &protocol::Tile) -> CalicoResult<Arc<dyn ObjectStore>> {
        Ok(self.object_store.clone())
    }

    pub fn object_path_for<'a>(&self, tile: &protocol::Tile) ->  String {
        let token = Uuid::new_v4().to_string();
        // todo: support paths for multiple partition keys
        todo!("convert partition key to a string");
        let partition = 0;
        format!("{}/{:08}/{}",OBJECT_PATH, partition, token)
    }

    pub fn full_object_path_for<'a>(&self, tile: &protocol::Tile) -> String {
        let subpath = self.object_path_for(tile);
        format!("{}/{}", self.object_store_url, subpath)
    }

    pub async fn log_store_for(&self, _tile: &protocol::Tile) -> CalicoResult<Arc<dyn ObjectStore>> {
        Ok(self.object_store.clone())
    }

    pub async fn default_object_store(&self, ) -> CalicoResult<Arc<dyn ObjectStore>> {
        Ok(self.object_store.clone())
    }

    pub async fn transaction_log_for(&self, _tile: &protocol::Tile) -> CalicoResult<Arc<TransactionLog>> {
        Ok(self.log.clone())
    }

    pub async fn default_transaction_log(&self) ->  CalicoResult<Arc<TransactionLog>> {
        Ok(self.log.clone())
    }

    pub async fn column_group_for_column(&self, column: &str) -> CalicoResult<String> {
        let col_meta = self.column_group(column).await?;
        Ok(col_meta.column_group.clone())
    }

    pub async fn column_group_meta(&self, column_group: &str) -> CalicoResult<&protocol::ColumnGroupMetadata> {
        self.column_group_config.get(column_group).ok_or(CalicoError::UnknownColumnGroup(column_group.to_string()))
    }

    pub async fn column_group(&self, column: &str) -> CalicoResult<&protocol::ColumnMetadata> {
        self.column_config.get(column).ok_or(CalicoError::UnknownColumn(column.to_string()))
    }

    pub async fn add_column_group(&mut self, column_group_meta: protocol::ColumnGroupMetadata) -> CalicoResult<()> {
        self.column_group_config.insert(column_group_meta.column_group.clone(), column_group_meta);
        Ok(())
    }

    pub async fn add_column(&mut self, column_meta: protocol::ColumnMetadata) -> CalicoResult<()> {
        self.column_config.insert(column_meta.column.clone(), column_meta);
        Ok(())
    }

    // Use Table configuration to determine the map of column groups to columns for the table
    pub async fn extract_column_groups(&self, schema:SchemaRef) -> CalicoResult<Vec<(String, Vec<usize>)>> {

        let field_names = schema.fields().iter().map(|field| field.name()).collect::<Vec<&String>>();

        let mut column_groups:Vec<Option<String>> = vec![];
        
        // load the column groups for all fields, None will mean it's an ID (shared across columns)
        for field_name in field_names.iter() {
            column_groups.push(self.column_group_for_column(field_name).await.ok());
        }

        // now map this to column_group -> vec[indices]
        let column_groups:Vec<(String, Vec<usize>)> = column_groups.iter()
            .enumerate()
            .group_by(|(_, column_group)| *column_group)
            .into_iter()
            .map(|(key, group)| (key.clone(), group.map(|(column_index, _)| column_index).collect::<Vec<usize>>()))
            .filter_map(|(opt_column_group, ids)| match opt_column_group {
                Some(column_group) => Some((column_group, ids)),
                None => None
            })
            .collect::<Vec<(String, Vec<usize>)>>();

        let mut output = vec![];

        // now load and prepend the ID field indices as well
        for (column_group, field_indices) in column_groups {
            let column_group_config = self.column_group_meta(column_group.as_str()).await?;
            let mut all_indices = column_group_config.id_columns.iter().map(|id_field| {
                match field_names.iter().position(|field_name| *field_name == id_field) {
                    Some(id_index) => id_index,
                    None => panic!("missing required id field for batch")
                }
            }).collect::<Vec<usize>>();

            all_indices.extend(field_indices.iter().cloned());

            output.push((column_group, all_indices));
        }

        Ok(output)
    }

        /*
            self.column_group_meta(column_group_name.as_str()).await?

        .iter().map(|f)

        let mut column_group_map = HashMap::<String, Vec<usize>>::new();

        for (field_idx, field_name) in field_names.iter().enumerate() {
            match self.column_group_for_column(field_name).await {
                Ok(column_group) => {
                    match column_group_map.get(&column_group) {
                        None => {
                            let column_group_config = self.column_group_meta(column_group.as_str()).await?;
                            let mut column_indices = column_group_config.id_columns.iter().map(|id_field| {
                                match field_names.iter().position(|field_name| *field_name == id_field) {
                                    Some(id_index) => id_index,
                                    None => panic!("missing required id field for batch")
                                }


                            }).collect::<Vec<usize>>();

                            column_indices.push(field_idx);

                            column_group_map.insert(column_group, column_indices);
                        },
                        Some(column_indices) => {
                            column_indices.push(field_idx);
                        }
                    }
                },
                Err(_) => {}
            }
        };
 */
}



impl Table {
    // Use Table configuration to determine the map of column groups to columns for the table
    pub async fn extract_column_groups(&self) -> CalicoResult<Vec<(String, Vec<usize>)>> {
        self.store.extract_column_groups(self.schema.clone()).await
    }
}

