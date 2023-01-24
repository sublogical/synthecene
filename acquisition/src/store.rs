use std::{path::PathBuf, sync::Arc};
use object_store::ObjectStore;

use crate::fetch::Capture;

#[derive(Debug)]
pub enum Error {
}

pub type Result<T> = std::result::Result<T, Error>;


pub struct LocalStore {}

impl LocalStore {
    pub fn add_capture(&self, _capture: &Capture) -> Result<()> {
        Ok(())
    }
    
    pub fn add_outlinks(&self, _outlinks: &Vec<String>) -> Result<()> {
        Ok(())
    }

    pub async fn upload(&self, _remote_path:&PathBuf, _object_store: Arc<dyn ObjectStore>) -> Result<()> {
        Ok(())
    }

    pub async fn maybe_upload(&self, _remote_path:&PathBuf, _object_store: Arc<dyn ObjectStore>) -> Result<()> {
        Ok(())
    }
}