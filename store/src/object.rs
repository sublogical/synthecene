use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::{future, Future};
use object_store::path::Path as ObjectStorePath;
use rand::Rng;

use crate::{result::{CalicoResult, CalicoError}, protocol};


const OBJECT_PATH: &'static str = "objects";

//TODO: consider using         let token = Uuid::new_v4().to_string();

pub fn object_path_for<'a>(tile: &protocol::Tile) ->  CalicoResult<ObjectStorePath> {
    let object_id = rand::thread_rng().gen::<[u8; 20]>().to_vec();
    let hexencode = hex::encode(&object_id);
    let path: ObjectStorePath = format!("{}/{}",OBJECT_PATH, hexencode).try_into().unwrap();
    Ok(path)
}
