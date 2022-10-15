use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::{future, Future};

use crate::partition::{Tile, ColumnGroupRef};
use crate::result::{CalicoResult, CalicoError};


pub fn to_path(path_prefix: &str, column_group: &ColumnGroupRef, partition_num: u64) -> String {
    format!("{}/{}/{:08x}", path_prefix, column_group, partition_num)
}

pub struct ObjectStore {
}

impl ObjectStore {
    pub fn get_writer(&self) -> ObjectWriter {
        ObjectWriter {}
    }
}

pub struct ObjectWriter {
}




pub async fn write_batches<'a>(object_store: &ObjectStore, split_batches: Vec<(Tile, Arc<RecordBatch>)>) -> CalicoResult<Vec<(Tile, String)>> {
    let handles = split_batches.iter().map(|(tile, batch)| {
        let writer = object_store.get_writer();

        let batch = batch.clone();
        let tile = tile.clone();

        tokio::spawn(async move {
            Ok(writer.write(tile, batch).await?)
        })
    });

    let output = future::try_join_all(handles).await?;
    output.into_iter().collect::<CalicoResult<Vec<(Tile, String)>>>()
}

impl <'a> ObjectWriter {
    pub fn write(&self, tile: Tile, batch: Arc<RecordBatch>) -> impl Future<Output=CalicoResult<(Tile, String)>> + 'a {
        let path = to_path("soo", &tile.column_group, tile.partition_num);

        async {
            Ok::<_,CalicoError>((tile, path))
        }
    }
}