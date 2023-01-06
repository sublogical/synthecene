use std::collections::HashMap;
use std::path::Path;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::SystemTime;
use acquisition::result::IndigoError;
use acquisition::result::IndigoResult;
use futures::FutureExt;
use futures::Stream;
use futures::stream::StreamFuture;
use yaque::channel;
use yaque::queue;

pub struct FrontierSender(yaque::Sender);

impl FrontierSender {
    pub async fn append_paths(&mut self, paths: &Vec<String>) -> IndigoResult<()> {
        let paths_bytes = paths.iter().map(|s| s.as_bytes().to_vec());
        
        self.0.send_batch(paths_bytes).await?;
        Ok(())
    }    
}
pub struct FrontierReceiver(yaque::Receiver);

impl Iterator for FrontierReceiver {
    type Item = IndigoResult<String>;

    /// Returns an iterator that always immediately returns.
    /// 
    /// None - indicates nothing is available
    /// Some(Ok(_)) - indicates a URL is available
    /// Some(Err(_)) - indicates something went wrong and you should freak out
    /// 
    fn next(&mut self) -> Option<Self::Item> {
        self.0.recv().now_or_never()
            .map(|result| match result {
                Ok(gaurd) => {
                    let output = from_utf8(&*gaurd).map(|url| url.to_string())?;
                    match gaurd.commit() {
                        Err(err) => {
                            return Err(IndigoError::from(err));
                        },
                        _ => {}
                    }
                    Ok(output)
                },
                Err(err) => {
                    Err(IndigoError::from(err))
                }
            })
    }
}

/// Frontier contains the queue(s) of prioritized & sorted URLs to be captured
pub struct FrontierStore {
    pub sender: FrontierSender,
    pub receiver: FrontierReceiver,

}

use async_stream::stream;
use futures_util::stream::StreamExt;

pub fn stream<'stream, 'store : 'stream> (receiver: &'store mut yaque::Receiver) -> impl Stream<Item = IndigoResult<String>> + 'stream {
    stream! {
        loop {
            match receiver.recv().await {
                Ok(gaurd) => {
                    let transformed = match from_utf8(&*gaurd) {
                        Ok(url) => url.to_string(),
                        Err(err) => {
                            yield Err(IndigoError::from(err));
                            break;
                        }

                    };
                    yield Ok(transformed);
                    match gaurd.commit() {
                        Err(err) => {
                            yield Err(IndigoError::from(err));
                            break;
                        },
                        _ => {}
                    }
                },
                Err(err) => {
                    yield Err(IndigoError::from(err));
                    break
                }
            }
        }
    }
}

impl FrontierStore {
    pub fn init_local(path: &Path) -> FrontierStore {
        let (sender, receiver) = channel(path).unwrap();
        FrontierStore { 
            sender: FrontierSender(sender),
            receiver: FrontierReceiver(receiver)
         }
    }
}


#[derive(Clone, Debug)]
pub struct LastVisit {
    fetch_time: SystemTime,
    status: u32
}

#[derive(Clone, Debug, Default)]
pub struct LastVisitStore {
    store: HashMap<String, LastVisit>
}

impl LastVisitStore {
    pub fn init_local(_path: &Path) -> LastVisitStore {
        LastVisitStore {
            store: HashMap::new()
        }
    }
}

