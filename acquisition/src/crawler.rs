use std::sync::Arc;

use fetch::controller::parse_cli_controller;
use object_store::{local::LocalFileSystem, ObjectStore};
use tempfile::tempdir;

mod fetch;
mod telemetry;

#[tokio::main]
async fn main() -> fetch::task::Result<()> {
    let mut controller = parse_cli_controller();

    // Simple single thread task loop
    // todo: make this support running multiple tasks simultaneously
    // todo: make this support limiting total resources by expected throughput per task
    loop {
        match controller.next_task().await {
            Some(mut task) => {
                // todo: use remote manifest to learn location for this tasks's object store & transaction log
                let temp_dir = tempdir().unwrap();
                let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
            
                let _report = task.run(object_store.clone()).await?;
                ()
            },
            None => break,
        }
    }
    Ok(())
}
