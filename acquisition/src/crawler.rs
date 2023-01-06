use acquisition::result::IndigoResult;
use fetch::controller::parse_cli_controller;

mod fetch;

#[tokio::main]
async fn main() -> IndigoResult<()> {
    let mut controller = parse_cli_controller();

    // Simple single thread task loop
    // todo: make this support running multiple tasks simultaneously
    // todo: make this support limiting total resources by expected throughput per task
    loop {
        match controller.next_task().await {
            Some(mut task) => {
                let _report = task.run().await?;
                ()
            },
            None => break,
        }
    }
    Ok(())
}
