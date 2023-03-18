use arrow::datatypes::SchemaRef;
use async_stream::stream;
use bytes::Bytes;
use futures::Stream;
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use uuid::Uuid;
use std::cmp::min;
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;

use arrow::{datatypes::Schema};
use arrow::record_batch::RecordBatch;
use futures::stream::StreamExt;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::partition::{SendableRecordBatchStream, Tile};
use crate::protocol;
use crate::table::OBJECT_PATH;

trait StoreWriter {
    fn write_batch(&mut self, batch:RecordBatch);
    fn close(&mut self);
}

struct ParquetWriter {
    writer: ArrowWriter<File>
}

impl ParquetWriter {
    pub fn _from_file(file: File, schema: Arc<Schema>) -> Self {
        let props = WriterProperties::builder().build();
        let writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();

        Self { writer }
    }
}

impl StoreWriter for ParquetWriter {
    fn write_batch(&mut self, batch:RecordBatch) {
        self.writer.write(&batch).expect("Writing batch");
    }

    fn close(&mut self) {
    }
}

fn estimate_rows_to_write(batch: &RecordBatch, max_bytes: usize, row_written:usize) -> usize {
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let total_bytes = buffer.len();
    let bytes_per_row = total_bytes as f32 / batch.num_rows() as f32;
    let total_rows_to_write = (max_bytes as f32 / bytes_per_row) as usize;
    let rows_to_write = total_rows_to_write - row_written as usize;

    rows_to_write
}

pub(crate) fn stream_batches_to_bytes(
    max_bytes: usize,
    schema: SchemaRef, 
    mut input: SendableRecordBatchStream,
) -> Pin<Box<dyn Stream<Item = Bytes> + Send>>
{
    Box::pin(stream! {
        let mut pending = None::<RecordBatch>;
        let mut continue_stream = true;

        while continue_stream {
            let mut buffer = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), None).unwrap();
            let mut written_rows = 0;
            let mut continue_file = true;

            while continue_file && continue_stream {
                let mut next_batch = pending.take();
                if next_batch.is_none() {
                    next_batch = input.next().await;
                }

                if let Some(batch) = next_batch {
                    let batch_rows = batch.num_rows();
                    let rows_to_write = estimate_rows_to_write(&batch, max_bytes, written_rows);

                    if (rows_to_write == 0) {
                        pending = Some(batch);
                        continue_file = false;
                    }
                    else if (rows_to_write < batch_rows) {
                        // writing a subset of the batch, slice it first
                        let subbatch = batch.slice(0, rows_to_write);
                        writer.write(&subbatch)
                            .expect("Writing batch");

                        pending = Some(batch.slice(rows_to_write, batch_rows - rows_to_write));
                        continue_file = false;
                    } else {
                        // writing the entire batch
                        writer.write(&batch)
                            .expect("Writing batch");
                    };

                    written_rows += min(rows_to_write, batch_rows);

                    // TODO: consider implementing AsyncWriter support in Arrow, so we can
                    // mp-stream row blocks to the object store. as written, this requires
                    // the entire partfile to fit in memory
                } else {
                    continue_stream = false;
                }
            }

            writer.close().unwrap();

            yield Bytes::from(buffer);
        }
    })
}

pub(crate) fn stream_bytes_to_objects<'a: 'b, 'b>(
    tile: &'a Tile,
    object_store: Arc<dyn ObjectStore>,
    store_path: &'a str,
    commit_id: &'a Vec<u8>,
    job_uuid: &'a Uuid,
    start_partnum: usize,
    input: &'a mut Pin<Box<dyn Stream<Item = Bytes> + Send>>,
) -> Pin<Box<dyn Stream<Item = protocol::TileFiles> + Send + 'b>> 
{
    let partition_path = tile.1.iter().map(|x| x.to_string()).collect::<Vec<String>>().join("/");
    let commit_id = hex::encode(commit_id);

    let prefix = format!("{}/{}/{:08}/{}/{}",
        store_path,
        OBJECT_PATH, 
        commit_id, 
        tile.0,
        partition_path);

    let mut partnum = start_partnum;

    Box::pin(stream! {
        while let Some(bytes) = input.next().await {
            let object_path = format!("{prefix}/part-{partnum:05}-{job_uuid}-c000.snappy.parquet");
            let object_path: ObjectStorePath = object_path.try_into().unwrap();

            let mut file = protocol::File::default();

            file.file_path = object_path.to_string();
            file.file_type = protocol::FileType::Data as i32;
            file.file_size = bytes.len().try_into().unwrap();
        
            // todo: timestamp? do we care?
        
            object_store.put(&object_path, bytes).await
                .expect("Writing object");

            let mut tile_files = protocol::TileFiles::default();
            let prot_tile: protocol::Tile = tile.clone().into();
            tile_files.tile = Some(prot_tile);
            tile_files.file.push(file);

            yield tile_files;

            partnum += 1;
        }
    })
}

