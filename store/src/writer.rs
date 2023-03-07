use arrow::array::Int32Array;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use datafusion::scalar::ScalarValue;
use futures::Stream;
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use uuid::Uuid;
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;

use arrow::{datatypes::Schema};
use arrow::record_batch::RecordBatch;
use futures::stream::{self, StreamExt };
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;

use calico_shared::result::CalicoResult;
use crate::partition::{SendableRecordBatchStream, Tile};
use crate::protocol;
use crate::table::{TableStore, OBJECT_PATH};

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

// Writes a single RecordBatch into an in-memory parquet file
pub(crate) fn write_batch_to_bytes(batch: Arc<RecordBatch>) -> CalicoResult<Bytes> {
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();

    writer.write(&batch)?;
    writer.close()?;

    Ok(Bytes::from(buffer))
}
use async_stream::stream;

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


pub(crate) fn stream_batches_to_bytes<'a>(
    schema: SchemaRef, 
    input: &'a mut SendableRecordBatchStream,
    max_bytes: usize
) -> Box<dyn Stream<Item = Bytes> + Send + Sync + 'a> 
{
    Box::new(stream! {
        let mut pending = None::<RecordBatch>;
        let mut continue_stream = true;

        while continue_stream {
            let mut buffer = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), None).unwrap();
            let mut written_rows = 0;

            if let Some(batch) = &pending {
                writer.write(&batch).unwrap();
                pending = None;
            }

            while pending.is_none() && continue_stream {
                if let Some(result) = input.next().await {
                    match result {
                        Ok(batch) => {
                            let batch_rows = batch.num_rows();
                            let rows_to_write = estimate_rows_to_write(&batch, max_bytes, written_rows);

                            if (rows_to_write == 0) {
                                pending = Some(batch);
                            }
                            else if (rows_to_write < batch_rows) {
                                // writing a subset of the batch, slice it first
                                let subbatch = batch.slice(0, rows_to_write);
                                writer.write(&subbatch)
                                    .expect("Writing batch");

                                pending = Some(batch.slice(rows_to_write, batch_rows - rows_to_write));
                            } else {
                                // writing the entire batch
                                writer.write(&batch)
                                    .expect("Writing batch");
                            };

                            // TODO: consider implementing AsyncWriter support in Arrow, so we can
                            // mp-stream row blocks to the object store. as written, this requires
                            // the entire partfile to fit in memory
                        },
                        Err(e) => {
                            println!("Error reading batch: {}", e);
                            continue_stream = false;
                        }
                    }
               } else {
                    continue_stream = false;
                }
            }

            writer.close().unwrap();

            yield Bytes::from(buffer);
        }
    })
}

pub(crate) fn stream_bytes_to_objects<'a>(
    tile: &Tile,
    object_store: Arc<dyn ObjectStore>,
    store_path: &'a str,
    commit_id: &'a Vec<u8>,
    job_uuid: &'a Uuid,
    start_partnum: usize,
    input: &'a mut Pin<Box<dyn Stream<Item = Bytes> + Send + 'a>>,
) -> Box<dyn Stream<Item = protocol::TileFiles> + Send + 'a> 
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

    Box::new(stream! {
        while let Some(bytes) = input.next().await {
            let object_path = format!("part-{partnum:05}-{job_uuid}-c000.snappy.parquet");
            let object_path: ObjectStorePath = object_path.try_into().unwrap();

            let mut file = protocol::File::default();

            file.file_path = object_path.to_string();
            file.file_type = protocol::FileType::Data as i32;
            file.file_size = bytes.len().try_into().unwrap();
        
            // todo: timestamp? do we care?
        
            object_store.put(&object_path, bytes).await
                .expect("Writing object");

            let mut tile_files = protocol::TileFiles::default();
            tile_files.file.push(file);

            yield tile_files;
        }
    })
}

pub(crate) async fn write_batch(table_store: &TableStore, tile: &protocol::Tile, batch: Arc<RecordBatch>) -> CalicoResult<protocol::File> {
    let data_store = table_store.data_store_for(tile).await?;

    // todo: make path
    // /{store-path}/object/{commit.uuid}/{cf}/{slice}/part-{partnum:05}-{job.uuid}-c000.snappy.parquet
    let object_path: ObjectStorePath = table_store.object_path_for(&tile).try_into().unwrap();

    let serialized_batch = write_batch_to_bytes(batch)?;

    let mut file = protocol::File::default();

    file.file_path = object_path.to_string();
    file.file_type = protocol::FileType::Data as i32;
    file.file_size = serialized_batch.len().try_into().unwrap();

    // todo: timestamp? do we care?

    data_store.put(&object_path, serialized_batch).await?;

    Ok(file)
}

// Write all the split batches in parallel to the object store(s)
pub(crate) async fn write_batches(table_store: &TableStore, 
                                  split_batches: &Vec<(protocol::Tile, Arc<RecordBatch>)>) -> 
                                  CalicoResult<Vec<(protocol::Tile, protocol::File)>> {

    // stream all of the batches for write
    let output = stream::iter(split_batches)
        .then(|(tile, batch)| async move {
            let prot_file = write_batch(&table_store, &tile, batch.clone()).await?;
            let tuple = (tile.clone(), prot_file);

            Ok(tuple)
        })
        .collect::<Vec<CalicoResult<(protocol::Tile, protocol::File)>>>().await;

    // Invert from Vec<Result> -> Result<Vec>
    output.into_iter().collect::<CalicoResult<Vec<(protocol::Tile, protocol::File)>>>()
}

