use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::{Result as DataFusionResult, DataFusionError};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion::physical_plan::file_format::FileScanConfig;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;
use arrow::datatypes::{SchemaRef as ArrowSchemaRef, Schema as ArrowSchema};
use datafusion::datasource::TableProvider;
use itertools::Itertools;
use object_store::{ObjectStore, ObjectMeta};
use object_store::path::Path as ObjectStorePath;

use crate::log::{TransactionLog, ReferencePoint, TableAction};
use crate::protocol;
use crate::result::{CalicoResult, CalicoError};

// todo: convert this to calico-specific schema objects to allow for protobuf types & column metadata
type Schema = ArrowSchema;
type SchemaRef = ArrowSchemaRef;

pub struct TableStore {
    object_store_url: ObjectStoreUrl,
    object_store: Arc<dyn ObjectStore>,
    log: Arc<TransactionLog>,

    column_group_config: HashMap<String, protocol::ColumnGroupMetadata>,
    column_config: HashMap<String, protocol::ColumnMetadata>,
}

type TableStoreRef = Arc<TableStore>;

pub struct Table {
    store: TableStoreRef,
    schema: SchemaRef,
    reference: ReferencePoint,
}

pub const ID_INDEX:usize = 0;
pub const ID_FIELD:&str = "id";

fn timestamp_to_datetime(timestamp: u64) -> DateTime<Utc> {
    // todo: move to a utility
    let ts_secs:i64 = (timestamp / 1000).try_into().unwrap();
    let ts_ns = (timestamp % 1000) * 1_000_000;

    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(ts_secs, ts_ns as u32), Utc)
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

fn make_partitioned_file(partition: u64, file: &protocol::File) -> PartitionedFile {
    // todo: support column-based partitioning
    let partition_values = vec![ScalarValue::UInt64(Some(partition))];

    PartitionedFile {
        object_meta: file.into(),
        partition_values,
        range: None,
        extensions: None,
    }
}

fn make_table_for_action(action:&TableAction, column_group:&str) -> Vec<Vec<PartitionedFile>> {
    let tile_files = match action {
        TableAction::Checkpoint(checkpoint) => &checkpoint.tile_files,
        TableAction::Commit(commit) => &commit.tile_files
    };

    tile_files.iter()
        .filter_map(|tile_file|{
            let tile = tile_file.tile.as_ref().expect("Should never have a tile_file with no tile");
            
            if tile.column_group != column_group {
                None
            } else {
                let partition_num = tile.partition_num;

                let part_files_for_tile = tile_file.file.iter().map(|file| 
                    make_partitioned_file(partition_num, file))
                    .collect::<Vec<PartitionedFile>>();

                Some(part_files_for_tile)
            }
        })
        .collect::<Vec<Vec<PartitionedFile>>>()
}

fn make_stats_for_action(_action:&TableAction, _column_group:&str) -> Statistics  {
    // todo: read stats from the transaction log?
    Statistics::default()
}

async fn make_schema_for_action(object_store:&Arc<dyn ObjectStore>, action:&TableAction, column_group:&str) -> Arc<Schema> {
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

        let mut column_groups = vec![];

        // Map columns to column group names, keeping ID untouched so that we preserve indexes into the field schema
        for field in schema.fields() {
            if field.name() != ID_FIELD {
                column_groups.push(self.column_group_for_column(field.name()).await?);
            } else {
                column_groups.push(ID_FIELD.to_string());
            }
        }

        // now map this to column_group -> vec[indices]
        let output = column_groups.iter().enumerate()
            .group_by(|(_, column_group)| *column_group)
            .into_iter()
            .filter(|(key, _)| *key != ID_FIELD)
            .map(|(key, group)| (key.clone(), group.map(|(column_index, _)| column_index).collect::<Vec<usize>>()))
            .collect::<Vec<(String, Vec<usize>)>>();

        Ok(output)
    }
}

impl Table {
    // Use Table configuration to determine the map of column groups to columns for the table
    pub async fn extract_column_groups(&self) -> CalicoResult<Vec<(String, Vec<usize>)>> {
        self.store.extract_column_groups(self.schema.clone()).await
    }

    pub fn define(store: Arc<TableStore>,
                  schema: Arc<Schema>,
                  reference: ReferencePoint) -> CalicoResult<Arc<dyn TableProvider>> {
        Ok(Arc::new(Table {
            store,
            schema,
            reference
        }))
    }

}

impl From<CalicoError> for DataFusionError {
    fn from(err: CalicoError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}

#[async_trait]
impl TableProvider for Table {
    fn as_any(&self) ->  &dyn std::any::Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        // todo: this becomes a view when it goes to a join
        TableType::Base
    }

    async fn scan(
        &self,
        _session: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        
        // get the set of column_groups that exist in the calico_schema requested
        let column_groups = self.extract_column_groups().await?;

        // for now just one column group supported
        assert_eq!(column_groups.len(), 1);
        let column_group = &column_groups[0].0;

        // todo[split_transaction_log]: map the column groups to the set of transaction logs that we should query

        let transaction_log = self.store.default_transaction_log().await?;
        let commit = transaction_log.find_commit(&self.reference).await?;
        let table = commit.view(100).await?;
        let actions = table.actions();

        // for now just one action supported
        assert_eq!(actions.len(), 1);

        let file_groups = make_table_for_action(&actions[0], column_group);
        let statistics = make_stats_for_action(&actions[0], column_group);
        let file_schema = make_schema_for_action(&self.store.object_store, &actions[0], column_group).await;

        // todo: when we support open partition columns, update this to match
        let table_partition_cols = vec![ID_FIELD.to_string()];

        ParquetFormat::default()
            .create_physical_plan(
                FileScanConfig {
                    object_store_url: self.store.object_store_url.clone(),
                    file_schema,
                    file_groups,
                    statistics,
                    projection: projection.clone(),
                    limit,
                    table_partition_cols,
                },
                filters,
            )
            .await

    }

    fn get_table_definition(&self) -> Option< &str>{
        None
    }

    fn supports_filter_pushdown(&self,_filter: &datafusion::prelude::Expr,) -> datafusion::error::Result<datafusion::logical_expr::TableProviderFilterPushDown>{
      Ok(datafusion::logical_expr::TableProviderFilterPushDown::Unsupported)
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, Float32Array};
    use arrow::datatypes::{DataType, Schema, Field};
    use arrow::record_batch::RecordBatch;
    use datafusion::from_slice::FromSlice;
    use datafusion::prelude::SessionContext;
    use tempfile::tempdir;
    use crate::log::ReferencePoint;
    use crate::operations::append_operation;
    use crate::table::{ID_FIELD, Table};
    use crate::test_util::*;

    #[tokio::test]
    async fn test_trivial_query() {

        let temp = tempdir().unwrap();
        let ctx = provision_ctx(temp.path());

        let col_groups = vec![COLGROUP_UNPARTITIONED];
        let table_store = provision_store(&ctx, &col_groups).await;

        append_operation(&table_store, &make_data(10,0, &col_groups)).await.unwrap();

        let reference = ReferencePoint::Mainline;

        let table_schema = Arc::new(Schema::new(vec![
            Field::new(ID_FIELD, DataType::Int64, false),
            Field::new(FIELD_C, DataType::Float32, false),
            Field::new(FIELD_D, DataType::Float32, false),
        ]));
        let table = Table::define(Arc::new(table_store), table_schema, reference).unwrap();    

        let table = ctx.register_table("test", table);

        let sql = format!("SELECT count(*), min({}), max({}) from test", FIELD_C, FIELD_D);

        let df = ctx
            .sql(sql.as_str())
            .await
            .unwrap();

        let expected = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("COUNT(UInt8(1))", DataType::Int64, true),
                Field::new("MIN(test.c)", DataType::Float32, true),
                Field::new("MAX(test.d)", DataType::Float32, true),
            ])),
            vec![
                Arc::new(Int64Array::from_slice(&[10])),
                Arc::new(Float32Array::from_slice(&[0.0])),
                Arc::new(Float32Array::from_slice(&[11.7])),
            ],
        ).unwrap();
        let actual: Vec<RecordBatch> = df.collect().await.unwrap();
        assert_eq!(actual.len(),1);
        assert_eq!(format!("{:?}", actual[0]), format!("{:?}", expected));

    }
}