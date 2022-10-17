use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::{future, Future};

use crate::result::{CalicoResult, CalicoError};


pub fn to_path(path_prefix: &str, column_group: &str, partition_num: u64) -> String {
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



impl <'a> ObjectWriter {
    pub fn write(&self, tile: Tile, _batch: Arc<RecordBatch>) -> impl Future<Output=CalicoResult<(Tile, String)>> + 'a {
        let path = to_path("soo", &tile.column_group, tile.partition_num);

        async {
            Ok::<_,CalicoError>((tile, path))
        }
    }
}