
use arrow::record_batch::RecordBatch;

use crate::log::MAINLINE;
use crate::table::{Table, TableStore};
use crate::{protocol, partition};
use crate::writer::write_batches;
use crate::partition::{ split_batch };
use crate::result::{CalicoResult};

pub async fn append_operation(table_store: &TableStore, 
                              batch:&RecordBatch) -> CalicoResult<Vec<protocol::Commit>> { 
    
    let split_batches = split_batch(table_store, batch).await?;
    let object_paths = write_batches(table_store, &split_batches).await?;

    let cols = vec!["a".to_string()];
    let col_expr = vec![("a".to_string(), "$new".to_string())];
    let mut commits = vec![];

    // todo: compress tile operations with the same transaction log into a single transaction

    for (tile, file) in object_paths {
        let log = table_store.transaction_log_for(&tile).await?;
        let head_id = log.head_id_mainline().await?;
        
        // todo: proper timestamp
        let timestamp = 0;

        let tile_files = protocol::TileFiles {
            tile: Some(tile),
            file: vec![file]
        };

        let commit = log.create_commit(
            &head_id.to_vec(), 
            None, 
            None, 
            None, 
            timestamp, 
            cols.to_vec(), 
            col_expr.to_vec(), 
            vec![tile_files]).await?;
    
        let _new_head = log.fast_forward(MAINLINE, &commit.commit_id).await.unwrap();

        commits.push(commit.commit);
    }

    Ok(commits)
}

/*

pub async fn read_operation(table: &CalicoTable,
                            schema: SchemaRef,
                            columns: Vec<String>,
                            reference: ReferencePoint) -> CalicoResult<Vec<RecordBatch>> {
    let ctx = SessionContext::new();

    // get the set of column_groups that exist in the calico_schema requested
    let column_groups = table.extract_column_groups(schema).await?;

    // for now just one column group supported
    assert_eq!(column_groups.len(), 1);

    // todo[split_transaction_log]: map the column groups to the set of transaction logs that we should query

    let transaction_log = table.default_transaction_log().await?;
    let commit = transaction_log.find_commit(reference).await?;
    let table = commit.view(100).await?;
    let actions = table.actions();

    type Partition = u64;

    #[derive(PartialEq)]
    enum Expression {
        Set,
        Delete
    }

    // todo: filter out tiles that don't contain a column group we care about
    // todo: apply a pruning predicate if appropriate

    let flattened_files:Vec<(String, usize, Expression, Partition, Vec<PartitionedFile>)> = actions.iter()
        .enumerate()
        .flat_map(|(action_idx, action)| {
            let (expression, tile_files) = match action {
                TableAction::Checkpoint(checkpoint) => {
                    // Checkpoints are always SET expressions
                    (Expression::Set, &checkpoint.tile_files)
                },
                TableAction::Commit(commit) => {
                    // todo: support other expressions on commit (e.g. append, delete)
                    (Expression::Set, &commit.tile_files)
                },
            };

            tile_files.iter()
                .filter_map(|tile_file|{

                tile_file.tile.as_ref().map(|tile| {
                (
                    tile.column_group.clone(),
                    action_idx,
                    Expression::Set,
                    tile.partition_num,
                    tile_file.file.iter().map(|file| make_partitioned_file(tile.partition_num, file))
                        .collect::<Vec<PartitionedFile>>()
                )
                })
            })
        })
        .collect::<Vec<(String, usize, Expression, Partition, Vec<PartitionedFile>)>>();

    // Group by col_group/action/partition and flatten lists of partitioned files, in case there are duplicate tiles in an action
    flattened_files.iter()
        .group_by(|(col_group, action_index, expression, partition, _)|
            (col_group, action_index, expression, partition))
        .into_iter()
        .map(|((col_group, action_index, expression, partition), iter)| {
            let mut partitioned_files = Vec::new();

            for (_,_,_,_, subset_partitioned_files) in iter {
                partitioned_files.extend(subset_partitioned_files);
            }
            (col_group, action_index, expression, partition, partitioned_files)
        })
        .collect::<Vec<(&String, &usize, &Expression, &Partition, Vec<&PartitionedFile>)>>();

    // Group by col_group/action and turn into tables
    flattened_files.iter()
        .group_by(|(col_group, action_index, expression, _, _)|
            (col_group, action_index, expression))
        .into_iter()
        .map(|((col_group, action_index, expression), iter)| {
            let table_files = iter.map(|(_,_,_,_,partitioned_files)| partitioned_files)
                .collect::<Vec<Vec<PartitionedFile>>>();

            // todo: get the expected schema for this tile from the tile & convert to an arrow schema
            // todo: build a projection to limit the columns to the ones we need

            let plan = ParquetFormat::default()
                .create_physical_plan(
                    FileScanConfig {
                        object_store_url,
                        file_schema,
                        file_groups: file_groups.into_values().collect(),
                        statistics: self.datafusion_table_statistics(),
                        projection: projection.clone(),
                        limit,
                        table_partition_cols,
                    },
                    filters,
                )
                .await;

            let mut partitioned_files = Vec::new();


    // todo: flatten out actions by building join
        
    flattened_files.iter()
        .group_by(|(col_group, _, _, _, _)| col_group)
        .into_iter()
        .map(|(col_group, iter)| {

            // For each column group, build the physical plan

            iter.map(|(_, action_idx, expr, partition, partition_files)|
                (action_idx, expr, partition, partition_files))
                .group_by(|(action_idx, expr, _, _)| (action_idx, expr))
                .into_iter()
                .map(|((action_idx, expr), iter)| {

                    let partition_iter:Iter<(Partition, Vec<PartitionedFile>)> =
                        iter.map(|(_,_,partition_num, partition_files)|(partition_num, partition_files));

                        // For each action, build the physical plan
                    let action_exec_plan = build_action_plan(partition_iter);


                    iter.map(|(_,_,partition_num, partition_files)| 
                        (partition_num, partition_files))
                        .group_by(|(partition, partitioned_files)| partition)
                        .into_iter()
                        .map(|(_, partition_files)| {

                            // For each partition, build a table
                            partition_files
                                .map(|(partition, partitioned_file)| partitioned_file)
                                .collect::<Vec<Vec<PartitionedFile>>>()

                        })
                        ;
                    
                })


        });




    // todo: map query to tiles
    
    // col_group -> [commit -> [(tile, file)]]

    // Convert (Tile,File) into PartitionedFile, organized by partitions
    // Vec<(column_group:String, Vec<(Commit, Vec<Vec<PartitionedFile>>)>)>
    let part_files:Vec<(String, TableAction, Partition, Vec<PartitionedFile>)>;

    let column_group_parts:Vec<(String, TableAction, Vec<Vec<PartitionedFile>>)>;

    // Convert Vec<Vec<PartitionedFile>> into a ExecutionPlan
    // Vec<(String, Vec<(Commit, ExecutionPlan)>)>
    let column_group_actions:Vec<(String, TableAction, Box<dyn ExecutionPlan>)>;
    
    // Join the commits, applying any operators smashed into a single multijoin
    // Vec<(String, Join(ExecutionPlan))>
    let column_group_tables:Vec<(String, Box<dyn ExecutionPlan>)>;

    // Join the column groups into a single table view
    // - support multi-dimensional joins:
    // - [a]       -> [DATA]
    // - [a][b]    -> [DATA]
    // - [a]   [c] -> [DATA]
    // does this even make sense? why not just treat different column groups as different tables?
    // Start by not considering them as separate tables and focus on the join across commits

    // Join(Join(ExecutionPlan)))
    

    //let tiles = tiles_for_read(calico_table, columns, reference).await?;

    //ListingTableConfig::new_with_multi_paths

    todo!()
}


 */

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use crate::operations::append_operation;
    use crate::test_util::*;

    #[tokio::test]
    async fn test_append() {
        let temp = tempdir().unwrap();
        let cols = vec![COLGROUP_1, COLGROUP_2];
        let table = provision_table(temp.path(), &cols).await;

        append_operation(&table, &make_data(10,0, &cols)).await.unwrap();
        append_operation(&table, &make_data(10,10, &cols)).await.unwrap();
        append_operation(&table, &make_data(10, 20, &cols)).await.unwrap();
        
        let log = table.default_transaction_log().await.unwrap();
        let head = log.head_mainline().await.unwrap();
        let history = head.history(100).await.unwrap();
        assert_eq!(history.len(), 9);
        // make sure the files are in the object store
    }
}