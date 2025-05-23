use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::pin::Pin;
use std::task::{Poll, Context};

use futures::Stream;
use pin_project::pin_project;

#[pin_project]
struct PartitionStream <P, I, O, E, F, S> 
where
    S: Stream<Item = I>,
{
    #[pin]
    source: S,
    new_children: Vec<Result<P,E>>,
    root_waker: Option<std::task::Waker>,
    child_buffer: HashMap<P, Vec<O>>,
    child_waker: HashMap<P, std::task::Waker>,
    partition: F,
}

pub trait PartitionStreamExt<P, O, E, F>: Stream
{
    fn partition_by(self, partition: F) -> PartitionStreamRoot<P, Self::Item, O, E, F, Self>
    where
        P: std::cmp::Eq + std::hash::Hash + Clone,
        F: Fn(Self::Item) -> Result<Vec<(P, O)>, E>,
        Self: Sized + Send,
    {
        let source = PartitionStream::new(self, partition);

        PartitionStreamRoot {
            source,
        }
    }
}

impl<P, O, E, F, S> PartitionStreamExt<P, O, E, F> for S where S: Stream + ?Sized {}

enum InnerPollResult {
    Pending,
    Ready,
    Done,
}


impl <P, I, O, E, F, S> PartitionStream <P, I, O, E, F, S> 
where
    P: std::cmp::Eq + std::hash::Hash + Clone,
    F: Fn(I) -> Result<Vec<(P, O)>, E>,
    S: Stream<Item = I>,
{
    fn new(source: S, partition: F) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            source,
            new_children: Vec::new(),
            root_waker: None,
            child_buffer: HashMap::new(),
            child_waker: HashMap::new(),
            partition,
        }))
    }
    fn poll_child(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        partition: P
    ) -> std::task::Poll<Option<O>> {
        if let Some(output) = self.as_mut().get_buffered_output(&partition) {
            // found existing buffered output for child
            return std::task::Poll::Ready(Some(output));
        }

        {   
            let this = self.as_mut().project();

            // setting waker for me
            this.child_waker.insert(partition.clone(), cx.waker().clone());
        }

        // no data, so we need to poll the source
        match self.as_mut().get_more(cx) {
            InnerPollResult::Ready => {
                if let Some(output) = self.as_mut().get_buffered_output(&partition) {
                    // now there is data for us
                    std::task::Poll::Ready(Some(output))
                } else {
                    // got more data, but still nothing for me
                    std::task::Poll::Pending
                }
            },
            InnerPollResult::Pending => {
                // still no data, but source is pending, let's wait
                std::task::Poll::Pending
            },
            InnerPollResult::Done => {
                // we didn't receive anything more data from the root and nothing is pending, we're done
                std::task::Poll::Ready(None)
            },
        }
    }

    fn poll_next_root(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>
    ) -> std::task::Poll<Option<Result<P,E>>> {
        if let Some(output) = self.as_mut().get_next_child() {
            return std::task::Poll::Ready(Some(output));
        }
        {   
            let this = self.as_mut().project();

            if this.root_waker.is_none() {
                *this.root_waker = Some(cx.waker().clone());
            }
        }

        // Nothing to return, try to get more data
        match self.as_mut().get_more(cx) {
            InnerPollResult::Ready => {
                if let Some(output) = self.get_next_child() {
                    // there is a new child, return it
                    std::task::Poll::Ready(Some(output))
                } else {
                    // got more data, but still no new child
                    std::task::Poll::Pending
                }
            },
            InnerPollResult::Done => {
                // we didn't receive anything more data from the root and nothing is pending, we're done
                std::task::Poll::Ready(None)
            },
            InnerPollResult::Pending => {
                // I'll need to keep waiting, waker should already be set
                std::task::Poll::Pending
            }
        }
    }

    fn get_buffered_output(
        self: Pin<&mut Self>,
        partition: &P
    ) -> Option<O> {
        let this = self.project();

        if let Some(buffer) = this.child_buffer.get_mut(&partition) {
            if let Some(item) = buffer.pop() {
                return Some(item);
            }
        }
        return None
    }
    
    fn get_next_child(self: Pin<&mut Self>) -> Option<Result<P, E>> {
        let this = self.project();

        this.new_children.pop()
    }

    fn get_more(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>
    ) -> InnerPollResult {
        let this = self.project();

        // todo: this should allow us to inspect the size of the buffers and decide whether to backpressure

        // If we have any buffered output waiting for a child stream to pick it up, don't request more
        if this.child_buffer.values().map(|v| v.len()).sum::<usize>() > 0 {
            return InnerPollResult::Ready;
        }

        // If we have any new children waiting to be spawned, don't request more
        if this.new_children.len() != 0 {
            return InnerPollResult::Ready;
        }

        match this.source.poll_next(cx) {
            Poll::Ready(Some(input)) => {
                // We got some output, call the proc to partition it
                match (this.partition)(input) {
                    Ok(output) => {
                        for (partition, item) in output {
                            if let Some(buffer) = this.child_buffer.get_mut(&partition) {
                                // Adding to an existing buffer, wake the existing child
                                buffer.push(item);
                                this.child_waker.remove(&partition).map(|waker| waker.wake());
                            } else {
                                // new child wake the root stream
                                this.child_buffer.insert(partition.clone(), vec![item]);
                                this.new_children.push(Ok(partition));
                                this.root_waker.take().map(|waker| waker.wake());
                            }
                        }        
                    },
                    Err(e) => {
                        // Error, wake the root stream
                        this.new_children.push(Err(e));
                        this.root_waker.take().map(|waker| waker.wake());
                    }
                }

                InnerPollResult::Ready
            },
            Poll::Ready(None) => {
                this.root_waker.take().map(|waker| waker.wake());
                let partitions = this.child_buffer.keys().cloned().collect::<Vec<_>>();
                for partition in partitions {
                    this.child_waker.remove(&partition).map(|waker| waker.wake());
                }
                InnerPollResult::Done
            },
            Poll::Pending => {
                InnerPollResult::Pending
            },
        }
    }

}

pub struct PartitionStreamRoot<P, I, O, E, F, S> 
where
    S: Stream<Item = I>,
{
    source: Arc<Mutex<PartitionStream<P, I, O, E, F, S>>>,
}

impl <P, I, O, E, F, S>  Stream for PartitionStreamRoot<P, I, O, E, F, S> 
where
    P: std::cmp::Eq + std::hash::Hash + Clone + Unpin + Send + Sync + 'static,
    I: 'static,
    O: Send + 'static,
    E: Send + 'static,
    F: Fn(I) -> Result<Vec<(P, O)>, E> + Send + 'static,
    S: Stream<Item = I> + Unpin + Send + 'static,
{
    type Item = Result<(P, Pin<Box<dyn Stream<Item=O> + Unpin + Send + Sync>>), E>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Ok(mut guard) = self.source.try_lock() {
            match PartitionStream::poll_next_root(Pin::new(&mut guard), cx) {
                Poll::Ready(Some(child)) => {
                    match child {
                        Ok(partition) => {
                            let partition_stream_child = PartitionStreamChild {
                                source: self.source.clone(),
                                partition: partition.clone(),
                            };
                            Poll::Ready(Some(Ok((partition, Box::pin(partition_stream_child)))))
                        },
                        Err(e) => Poll::Ready(Some(Err(e)))
                        
                    }
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}


pub struct PartitionStreamChild<P, I, O, E, F, S>
where
    S: Stream<Item = I>,
{
    source: Arc<Mutex<PartitionStream<P, I, O, E, F, S>>>,
    partition: P,
}

impl <P, I, O, E, F, S> Stream for PartitionStreamChild<P, I, O, E, F, S> 
where
    P: std::cmp::Eq + std::hash::Hash + Clone,
    F: Fn(I) -> Result<Vec<(P, O)>, E>,
    S: Stream<Item = I> + Unpin,

{
    type Item = O;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Ok(mut guard) = self.source.try_lock() {
            match PartitionStream::poll_child(Pin::new(&mut *guard), cx, self.partition.clone()) {
                Poll::Ready(Some(output)) => {
                    Poll::Ready(Some(output))
                },
                Poll::Ready(None) => {
                    Poll::Ready(None)
                },
                Poll::Pending => {
                    Poll::Pending
                }
            }
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}



#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use futures::stream;
    use futures::stream::StreamExt;
    use super::PartitionStreamExt;

    #[tokio::test]
    async fn basic_stream_partition_test() {
        let input = vec![
            vec![(1, 1), (2, 2), (3, 3)],
            vec![(1, 3), (2, 4), (3, 6), (4, 4)], // unseen partition can occur at any point
            vec![(1, 5), (2, 6), (3, 9), (4, 8), (5, 5)],
            vec![(4, 12), (5, 10), (6, 6)], // previously seen partitions need not be present
            vec![(5, 15), (6, 12)],
            vec![(1, 7), (2, 8)], // and can reappear later
        ];
        let expected = vec![
            (1, vec![1, 3, 5, 7]),
            (2, vec![2, 4, 6, 8]),
            (3, vec![3, 6, 9]),
            (4, vec![4, 8, 12]),
            (5, vec![5, 10, 15]),
            (6, vec![6, 12]),
        ];

        #[derive(Debug)]
        enum NoError {}

        let stream = stream::iter(input);
        let partitioned_stream = stream.partition_by(|record| {
            let mapped_record = record
                .into_iter()
                .map(|x| (x.0.clone(), x.1))
                .collect::<BTreeMap<_, _>>();
            let mapped_records = mapped_record.iter().map(|(key, value)| {(key.clone(), value.clone())}).collect::<Vec<_>>();
            Ok::<_,NoError>(mapped_records)
        });

        let handles = partitioned_stream.map(|result| async move {
            match result {
                Ok((partition, stream)) => {
                    let output = stream.collect::<Vec<i32>>().await;
                    (partition, output)
                }
                Err(e) => panic!("Error: {:?}", e),
            }
        }).map(|fut| {
            tokio::spawn(fut)
        }).collect::<Vec<_>>().await;

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            results.push(handle.await.unwrap());
        }

        results.sort();

        assert_eq!(results, expected);

    }
}