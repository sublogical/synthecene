use std::sync::Arc;

use arrow::compute::SortOptions;
use async_trait::async_trait;
use calico_shared::result::CalicoResult;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::TableProvider;
use datafusion::error::{Result as DataFusionResult, DataFusionError};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableType;
use datafusion::logical_plan::DFSchema;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sort_merge_join::SortMergeJoinExec;
use datafusion::physical_plan::{ExecutionPlan, Statistics, PhysicalExpr};
use datafusion::physical_plan::file_format::FileScanConfig;
use datafusion::prelude::{Expr, JoinType, coalesce, col};
use arrow::datatypes::{SchemaRef as ArrowSchemaRef, Schema as ArrowSchema};
use crate::log::{TableAction, ReferencePoint};
use crate::protocol;
use crate::table::{make_schema_for_action, Table, TableStore};

// todo: convert this to calico-specific schema objects to allow for protobuf types & column metadata
type Schema = ArrowSchema;
type SchemaRef = ArrowSchemaRef;

pub struct DataFusionTable(Table);

fn make_partitioned_file(partition_key: &Vec<protocol::PartitionValue>, file: &protocol::File) -> PartitionedFile {
    let partition_values = partition_key.into_iter().map(|v| v.clone().into()).collect();
    
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
                let part_files_for_tile = tile_file.file.iter().map(|file| 
                    make_partitioned_file(&tile.partition_key, file))
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



#[derive(Debug)]
struct StatisticsForMergeExec {
    inner: Arc<dyn ExecutionPlan>
}

impl ExecutionPlan for StatisticsForMergeExec {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn schema(&self) -> ArrowSchemaRef { self.inner.schema() }
    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning { self.inner.output_partitioning() }
    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> { self.inner.output_ordering() }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> { vec![self.inner.clone()] }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> DataFusionResult<datafusion::physical_plan::SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DataFusionTable {

    /**
     * Build a DataFusion ExecutionPlan for a single TableAction, which may be 
     * a checkpoint or a commit, applying the given filters and limit.
     */
    async fn build_action_plan(&self, 
                               _projection: &Option<Vec<usize>>,
                               filters: &[Expr],
                               limit: Option<usize>,
                               column_group:&str,
                               action:&TableAction) -> DataFusionResult<Arc<dyn ExecutionPlan>> {

        let file_groups = make_table_for_action(&action, column_group);
        let statistics = make_stats_for_action(&action, column_group);
        let file_schema = make_schema_for_action(&self.0.store.object_store, &action, column_group).await;

        // todo: when we support open partition columns, update this to match
        let table_partition_cols = vec![];

        // todo: use projection and self.table_schema to determine the set of fields we need from 
        // file_schema. Add those indices plus the ID indices to get sub_projection
        let sub_projection = None;

        ParquetFormat::default()
            .create_physical_plan(
                FileScanConfig {
                    object_store_url: self.0.store.object_store_url.clone(),
                    file_schema,
                    file_groups,
                    statistics,
                    projection: sub_projection,
                    limit,
                    table_partition_cols,
                },
                filters,
            )
            .await
    }

    fn merge_action_plan(&self, column_group: &protocol::ColumnGroupMetadata, _action:&TableAction, left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>) -> DataFusionResult<Arc<dyn ExecutionPlan>> {

        let on = column_group.id_columns.iter().map(|column| {
            let id_col_left = Column::new(column.as_str(), 0);
            let id_col_right = Column::new(format!("new_{column}").as_str(), 0);
            (id_col_left, id_col_right)
        }).collect::<Vec<(Column, Column)>>();
        let sort_options = vec![SortOptions::default(); on.len()];

        // map right cols to new_$col so that we can have a reasonable names for final expression
        let preproject_expr = right.schema().fields().iter().enumerate().map(|(index, field)| {
            let field_name = field.name();

            let expr:Arc<dyn PhysicalExpr> = Arc::new(Column::new(field_name, index));

            (expr, format!("new_{field_name}"))
        }).collect::<Vec<_>>();
        let right = Arc::new(ProjectionExec::try_new(preproject_expr, right)?);

        let merge = Arc::new(SortMergeJoinExec::try_new(left.clone(), right.clone(), on, JoinType::Full, sort_options, false)?);
        
        // Put the merge in a wrapper since there is a todo! in statistics for SortMergeJoinExec, frighteningly
        let wrapped_inner = Arc::new(StatisticsForMergeExec { inner: merge.clone() });

        // Now perform the logical projection. This is where new values overwrite old or whateever, typically coalese[new, old]

        let merge_schema = merge.schema();
        let merge_dfschema:DFSchema = DFSchema::try_from((*merge_schema).clone())?;
        let execution_props = ExecutionProps::new();

        let postproject_expr = left.schema().fields().iter().map(|field| {
            let field_name = field.name();

            let left_col = col(field_name);
            let right_col = col(format!("new_{field_name}").as_str());

            // For now, just use coalesce for everything. When we have merge expressions we'll stick that here

            // todo: use action to side the merge operation
            let logical_expr = coalesce(vec![right_col, left_col]);

            let physical_expr = create_physical_expr(
                &logical_expr,
                &merge_dfschema,
                &merge_schema,
                &execution_props,
            ).unwrap();

            (physical_expr, field_name.clone())
        }).collect::<Vec<_>>();

        let output = Arc::new(ProjectionExec::try_new(postproject_expr, wrapped_inner)?);

        Ok(output)
    }

    fn positional_projection(&self, positional:&Vec<usize>, schema:SchemaRef, exec: Arc<dyn ExecutionPlan>) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let fields = schema.fields();
        
        let projection:Vec<_> = positional.iter().map(|idx| {
            let name = fields[*idx].name();
            let col:Arc<dyn PhysicalExpr> = Arc::new(Column::new(name, *idx));
            
            (col, name.to_owned())
        }).collect();

        Ok(Arc::new(ProjectionExec::try_new(projection, exec)?))
    }

    pub fn define(store: Arc<TableStore>,
        schema: Arc<Schema>,
        reference: ReferencePoint) -> CalicoResult<Arc<dyn TableProvider>> {

        Ok(Arc::new(DataFusionTable(Table {
            store,
            schema,
            reference
        })))
    }

}

#[async_trait]
impl TableProvider for DataFusionTable {
    fn as_any(&self) ->  &dyn std::any::Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.0.schema.clone()
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
        let column_groups = self.0.extract_column_groups().await?;

        // for now just one column group supported
        assert_eq!(column_groups.len(), 1);
        let column_group = &column_groups[0].0;
        let column_group_meta = self.0.store.column_group_meta(&column_group.as_str()).await?;

        // todo[split_transaction_log]: map the column groups to the set of transaction logs that we should query

        let transaction_log = self.0.store.default_transaction_log().await?;
        let commit = transaction_log.find_commit(&self.0.reference).await?;
        let table = commit.view(100).await?;
        let actions = table.actions();

        let mut plan:Option<Arc<dyn ExecutionPlan>> = None;

        for action in actions {
            // todo: patch the projection such that we don't lose the ID column
            let subplan = self.build_action_plan(projection, filters, limit, column_group, &action).await?;

            plan = match plan {
                Some(prior) => Some(self.merge_action_plan(&column_group_meta, &action, prior, subplan)?),
                None => Some(subplan)
            }
        }

        // apply the final projection to get it to match the expected schema
        plan = match projection {
            Some(projection) => plan.and_then(|plan| Some(self.positional_projection(projection, plan.schema(), plan).ok()?)),
            None => plan
        };

        plan.ok_or(DataFusionError::Internal("Failed to load at least one action plan".to_string()))
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

    use arrow::record_batch::RecordBatch;
    use datafusion::{ assert_batches_sorted_eq};
    use std::future::Future;
    use tempfile::tempdir;
    use crate::log::ReferencePoint;
    use crate::datafusion::operation::append::{append_operation, append_operation_at};
    use crate::datafusion::table::DataFusionTable;
    use crate::table::Table;
    use crate::test_util::*;

    use super::TableStore;

    fn overlap_batch(idx: u8) -> RecordBatch {
        match idx {
            1 => build_table_i32(
                    (ID_FIELD, &vec![0, 1, 2]),
                    (FIELD_C,  &vec![3, 4, 5]),
                    (FIELD_D,  &vec![4, 5, 6]),
                ),
            2 => build_table_i32(
                    (ID_FIELD, &vec![0, 1, 2]),
                    (FIELD_C,  &vec![4, 5, 6]),
                    (FIELD_D,  &vec![7, 8, 9]),
                ),
            3 => build_table_i32(
                    (ID_FIELD, &vec![0, 1, 2]),
                    (FIELD_C,  &vec![7, 8, 9]),
                    (FIELD_D,  &vec![1, 1, 1]),
                ),
            _ => panic!("unknown test case")
        }
    }

    const OVERLAP_EXPECTED_1: &'static [&'static str] = &[
        "+----+---+---+",
        "| id | c | d |",
        "+----+---+---+",
        "| 0  | 3 | 4 |",
        "| 1  | 4 | 5 |",
        "| 2  | 5 | 6 |",
        "+----+---+---+",
    ];

    const OVERLAP_EXPECTED_2: &'static [&'static str] = &[
        "+----+---+---+",
        "| id | c | d |",
        "+----+---+---+",
        "| 0  | 4 | 7 |",
        "| 1  | 5 | 8 |",
        "| 2  | 6 | 9 |",
        "+----+---+---+",
    ];

    const OVERLAP_EXPECTED_3: &'static [&'static str] = &[
        "+----+---+---+",
        "| id | c | d |",
        "+----+---+---+",
        "| 0  | 7 | 1 |",
        "| 1  | 8 | 1 |",
        "| 2  | 9 | 1 |",
        "+----+---+---+",
    ];


    async fn test_runner<F, Fut>(col_groups: &Vec<&str>, fields: &Vec<&str>, sql: &str, lambda: F) -> Vec<RecordBatch> 
    where
        F: FnOnce(Arc<TableStore>) -> Fut,
        Fut: Future<Output = ()>
    {
        let temp = tempdir().unwrap();
        let ctx = provision_ctx(temp.path());

        let table_store = Arc::new(provision_store(&ctx, &col_groups).await);

        lambda(table_store.clone()).await;

        let reference = ReferencePoint::Main;
        let table_schema = make_schema(&fields);
        
        let table = DataFusionTable::define(table_store, table_schema, reference).unwrap();    
        ctx.register_table("test", table).unwrap();

        let df = ctx.sql(sql).await.unwrap();
        
        let actual: Vec<RecordBatch> = df.collect().await.unwrap();

        actual
    }
    #[tokio::test]
    async fn test_trivial_query() {
        let col_groups = vec![COLGROUP_UNPARTITIONED];
        let columns = vec![FIELD_C, FIELD_D];

        let batch_1 = build_table_i32(
            (ID_FIELD, &vec![0, 1, 2, 3, 4, 5, 6, 7, 8]),
            (FIELD_C,  &vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
            (FIELD_D,  &vec![9, 8, 7, 6, 5, 4, 3, 2, 1]),
        );

        let sql = format!("SELECT * FROM test");

        let actual = test_runner(&col_groups.clone(), &columns, sql.as_str(), |table_store| async move {
            append_operation(table_store, batch_1).await.unwrap();
        }).await;

        let expected = vec![
            "+----+---+---+",
            "| id | c | d |",
            "+----+---+---+",
            "| 0  | 1 | 9 |",
            "| 1  | 2 | 8 |",
            "| 2  | 3 | 7 |",
            "| 3  | 4 | 6 |",
            "| 4  | 5 | 5 |",
            "| 5  | 6 | 4 |",
            "| 6  | 7 | 3 |",
            "| 7  | 8 | 2 |",
            "| 8  | 9 | 1 |",
            "+----+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &actual);
    }

    #[tokio::test]
    async fn test_partitioned_single_commit() {
        let col_groups = vec![COLGROUP_PARTITIONED];
        let columns = vec![FIELD_A, FIELD_B];

        let batch_1 = build_table_i32(
            (ID_FIELD, &vec![0, 1, 2, 3, 4, 5, 6, 7, 8]),
            (FIELD_A,  &vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
            (FIELD_B,  &vec![9, 8, 7, 6, 5, 4, 3, 2, 1]),
        );

        let sql = format!("SELECT * FROM test");

        let actual = test_runner(&col_groups.clone(), &columns, sql.as_str(), |table_store| async move {
            append_operation(table_store, batch_1).await.unwrap();
        }).await;

        let expected = vec![
            "+----+---+---+",
            "| id | a | b |",
            "+----+---+---+",
            "| 0  | 1 | 9 |",
            "| 1  | 2 | 8 |",
            "| 2  | 3 | 7 |",
            "| 3  | 4 | 6 |",
            "| 4  | 5 | 5 |",
            "| 5  | 6 | 4 |",
            "| 6  | 7 | 3 |",
            "| 7  | 8 | 2 |",
            "| 8  | 9 | 1 |",
            "+----+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &actual);
    }
 
    /*
    #[tokio::test]
    async fn test_multiple_column_groups() {
        let col_groups = vec![COLGROUP_PARTITIONED, COLGROUP_UNPARTITIONED];
        let columns = vec![FIELD_A, FIELD_B, FIELD_C, FIELD_D];

        let actual = test_runner(&col_groups.clone(), &columns, |table_store| async move {
            append_operation(&table_store, &make_data(10,0, 0, &col_groups)).await.unwrap();
        }).await;

        let expected = make_expected(10,0.0,11.7);
        assert_eq!(format!("{:?}", actual[0]), format!("{:?}", expected));
    }
    */

    #[tokio::test]
    async fn test_non_overlapping_unpartitioned_commits() {
        let col_groups = vec![COLGROUP_UNPARTITIONED];
        let columns = vec![FIELD_C, FIELD_D];

        let batch_1 = build_table_i32(
            (ID_FIELD, &vec![0, 1, 2]),
            (FIELD_C,  &vec![1, 2, 3]),
            (FIELD_D,  &vec![4, 5, 6]),
        );

        let batch_2 = build_table_i32(
            (ID_FIELD, &vec![3, 4, 5]),
            (FIELD_C,  &vec![4, 5, 6]),
            (FIELD_D,  &vec![7, 8, 9]),
        );

        let batch_3 = build_table_i32(
            (ID_FIELD, &vec![6, 7, 8]),
            (FIELD_C,  &vec![7, 8, 9]),
            (FIELD_D,  &vec![1, 1, 1]),
        );

        let sql = format!("SELECT * FROM test");

        let actual = test_runner(&col_groups.clone(), &columns, sql.as_str(), |table_store| async move {
            append_operation(table_store.clone(), batch_1).await.unwrap();
            append_operation(table_store.clone(), batch_2).await.unwrap();
            append_operation(table_store.clone(), batch_3).await.unwrap();
        }).await;

        let expected = vec![
            "+----+---+---+",
            "| id | c | d |",
            "+----+---+---+",
            "| 0  | 1 | 4 |",
            "| 1  | 2 | 5 |",
            "| 2  | 3 | 6 |",
            "| 3  | 4 | 7 |",
            "| 4  | 5 | 8 |",
            "| 5  | 6 | 9 |",
            "| 6  | 7 | 1 |",
            "| 7  | 8 | 1 |",
            "| 8  | 9 | 1 |",
            "+----+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &actual);
    }

    #[tokio::test]
    async fn test_overlapping_unpartitioned_commits() {
        let col_groups = vec![COLGROUP_UNPARTITIONED];
        let columns = vec![FIELD_C, FIELD_D];
        let sql = format!("SELECT * FROM test");

        let actual = test_runner(&col_groups.clone(), &columns, sql.as_str(), |table_store| async move {
            append_operation(table_store.clone(), overlap_batch(1)).await.unwrap();
            append_operation(table_store.clone(), overlap_batch(2)).await.unwrap();
            append_operation(table_store.clone(), overlap_batch(3)).await.unwrap();
        }).await;

        assert_batches_sorted_eq!(OVERLAP_EXPECTED_3, &actual);
    }


    #[tokio::test]
    async fn _test_string_id_table() {
        let _batch2 = build_table(&vec![
            (ID_FIELD, str_col(&vec!["0", "1", "2", "3", "4", "5", "6", "7", "8"])),
            (FIELD_A, i32_col(&vec![31, 32, 33, 34, 35, 36, 37, 38, 39])),
            (FIELD_B, i32_col(&vec![41, 42, 43, 44, 45, 46, 47, 48, 49])),
        ]);

    }

    #[tokio::test]
    async fn test_alt_numeric_id_table() {
        let test_cases = vec![
            vec![
                build_table(&vec![
                    (ID_FIELD, i16_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                    (FIELD_A, i32_col(&vec![11, 12, 13, 14, 15, 16, 17, 18, 19])),
                    (FIELD_B, i32_col(&vec![21, 22, 23, 24, 25, 26, 27, 28, 29])),
                ]),
                build_table(&vec![
                    (ID_FIELD, i16_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                    (FIELD_A, i32_col(&vec![31, 32, 33, 34, 35, 36, 37, 38, 39])),
                    (FIELD_B, i32_col(&vec![41, 42, 43, 44, 45, 46, 47, 48, 49])),
                ]),
            ],
            vec![
                build_table(&vec![
                    (ID_FIELD, i32_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                    (FIELD_A, i32_col(&vec![11, 12, 13, 14, 15, 16, 17, 18, 19])),
                    (FIELD_B, i32_col(&vec![21, 22, 23, 24, 25, 26, 27, 28, 29])),
                ]),
                build_table(&vec![
                    (ID_FIELD, i32_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                    (FIELD_A, i32_col(&vec![31, 32, 33, 34, 35, 36, 37, 38, 39])),
                    (FIELD_B, i32_col(&vec![41, 42, 43, 44, 45, 46, 47, 48, 49])),
                ]),
            ],
            vec![
                build_table(&vec![
                    (ID_FIELD, i64_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                    (FIELD_A, i32_col(&vec![11, 12, 13, 14, 15, 16, 17, 18, 19])),
                    (FIELD_B, i32_col(&vec![21, 22, 23, 24, 25, 26, 27, 28, 29])),
                ]),
                build_table(&vec![
                    (ID_FIELD, i64_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                    (FIELD_A, i32_col(&vec![31, 32, 33, 34, 35, 36, 37, 38, 39])),
                    (FIELD_B, i32_col(&vec![41, 42, 43, 44, 45, 46, 47, 48, 49])),
                ]),
            ],
            vec![
                build_table(&vec![
                    (ID_FIELD, u16_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                    (FIELD_A, i32_col(&vec![11, 12, 13, 14, 15, 16, 17, 18, 19])),
                    (FIELD_B, i32_col(&vec![21, 22, 23, 24, 25, 26, 27, 28, 29])),
                ]),
                build_table(&vec![
                    (ID_FIELD, u16_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                    (FIELD_A, i32_col(&vec![31, 32, 33, 34, 35, 36, 37, 38, 39])),
                    (FIELD_B, i32_col(&vec![41, 42, 43, 44, 45, 46, 47, 48, 49])),
                ]),
            ],
            vec![
                build_table(&vec![
                    (ID_FIELD, u32_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                    (FIELD_A, i32_col(&vec![11, 12, 13, 14, 15, 16, 17, 18, 19])),
                    (FIELD_B, i32_col(&vec![21, 22, 23, 24, 25, 26, 27, 28, 29])),
                ]),
                build_table(&vec![
                    (ID_FIELD, u32_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                    (FIELD_A, i32_col(&vec![31, 32, 33, 34, 35, 36, 37, 38, 39])),
                    (FIELD_B, i32_col(&vec![41, 42, 43, 44, 45, 46, 47, 48, 49])),
                ]),
            ],
            vec![
                build_table(&vec![
                    (ID_FIELD, u64_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                    (FIELD_A, i32_col(&vec![11, 12, 13, 14, 15, 16, 17, 18, 19])),
                    (FIELD_B, i32_col(&vec![21, 22, 23, 24, 25, 26, 27, 28, 29])),
                ]),
                build_table(&vec![
                    (ID_FIELD, u64_col(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8])),
                    (FIELD_A, i32_col(&vec![31, 32, 33, 34, 35, 36, 37, 38, 39])),
                    (FIELD_B, i32_col(&vec![41, 42, 43, 44, 45, 46, 47, 48, 49])),
                ]),
            ],
        ];

        let expected = vec![
            "+----+----+----+",
            "| id | a  | b  |",
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

        for test_case in test_cases {
            let col_groups = vec![COLGROUP_PARTITIONED];
            let columns = vec![FIELD_A, FIELD_B];
            let sql = format!("SELECT * FROM test");
    
            let actual = test_runner(&col_groups.clone(), &columns, sql.as_str(), |table_store| async move {
                append_operation(table_store.clone(), test_case[0].clone()).await.unwrap();
                append_operation(table_store.clone(), test_case[1].clone()).await.unwrap();
            }).await;

            assert_batches_sorted_eq!(expected, &actual);
        }
    }

    #[tokio::test]
    async fn test_multi_id_table() {
    }
 
    #[tokio::test]
    async fn test_timetravel_read() {
        let temp = tempdir().unwrap();
        let ctx = provision_ctx(temp.path());

        let col_groups = vec![COLGROUP_UNPARTITIONED];
        let columns = vec![FIELD_C, FIELD_D];

        let table_store = Arc::new(provision_store(&ctx, &col_groups).await);

        let c1 = append_operation_at(table_store.clone(), 1000, overlap_batch(1)).await.unwrap();
        let c2 = append_operation_at(table_store.clone(), 2000, overlap_batch(2)).await.unwrap();
        let c3 = append_operation_at(table_store.clone(), 3000, overlap_batch(3)).await.unwrap();

        let table_schema = make_schema(&columns);

        let test_cases = vec![
            ("head",   ReferencePoint::Main,                                   OVERLAP_EXPECTED_3),
            // todo: implement ancestor reference search
            // ("anc",    ReferencePoint::Ancestor(Box::new(ReferencePoint::Mainline), 2), OVERLAP_EXPECTED_1),
            ("c1",     ReferencePoint::Commit(c1.commit_id),                       OVERLAP_EXPECTED_1),
            ("c2",     ReferencePoint::Commit(c2.commit_id),                       OVERLAP_EXPECTED_2),
            ("c3",     ReferencePoint::Commit(c3.commit_id),                       OVERLAP_EXPECTED_3),
            ("parent", ReferencePoint::Parent(Box::new(ReferencePoint::Main)), OVERLAP_EXPECTED_2),
            // todo: implement timestamp reference search
            // ("timestamp", ReferencePoint::TimestampFrom(Box::new(ReferencePoint::Mainline), 2500), OVERLAP_EXPECTED_2)
        ];

        for (test_name, point, expected) in test_cases {
            let table = DataFusionTable::define(table_store.clone(), table_schema.clone(), point).unwrap();
            let table_name = format!("test_{}", test_name);
            ctx.register_table(table_name.as_str(), table).unwrap();

            let sql = format!("SELECT * FROM {}", table_name);
            let df = ctx.sql(&sql).await.unwrap();        
            let actual: Vec<RecordBatch> = df.collect().await.unwrap();
            assert_batches_sorted_eq!(expected, &actual);    
        }
    }

    // todo: test support for reading at a particular commit in history
}