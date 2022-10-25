use object_store::path::Path as ObjectStorePath;
use rand::Rng;

use crate::{result::{CalicoResult}, protocol};


const OBJECT_PATH: &'static str = "objects";

//TODO: consider using         let token = Uuid::new_v4().to_string();

pub fn object_path_for<'a>(tile: &protocol::Tile) ->  CalicoResult<ObjectStorePath> {
    let object_id = rand::thread_rng().gen::<[u8; 20]>().to_vec();
    let partition = tile.partition_num;
    let hexencode = hex::encode(&object_id);
    let path: ObjectStorePath = format!("{}/{:08}/{}",OBJECT_PATH, partition, hexencode).try_into().unwrap();
    Ok(path)
}
