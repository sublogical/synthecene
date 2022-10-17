use bytes::Bytes;
use std::fs::File;
use std::sync::Arc;

use arrow::{datatypes::Schema};
use arrow::record_batch::RecordBatch;
use futures::stream::{self, StreamExt };
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::object::object_path_for;
use crate::protocol;
use crate::result::CalicoResult;
use crate::table::CalicoTable;

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

pub(crate) async fn write_batch(calico_table: &CalicoTable, tile: &protocol::Tile, batch: Arc<RecordBatch>) -> CalicoResult<protocol::File> {
    let data_store = calico_table.data_store_for(tile).await?;
    let object_path = object_path_for(&tile)?;

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
pub(crate) async fn write_batches(calico_table: &CalicoTable, 
                                  split_batches: &Vec<(protocol::Tile, Arc<RecordBatch>)>) -> 
                                  CalicoResult<Vec<(protocol::Tile, protocol::File)>> {

    // stream all of the batches for write
    let output = stream::iter(split_batches)
        .then(|(tile, batch)| async move {
            let prot_file = write_batch(&calico_table, &tile, batch.clone()).await?;
            let tuple = (tile.clone(), prot_file);

            Ok(tuple)
        })
        .collect::<Vec<CalicoResult<(protocol::Tile, protocol::File)>>>().await;

    // Invert from Vec<Result> -> Result<Vec>
    output.into_iter().collect::<CalicoResult<Vec<(protocol::Tile, protocol::File)>>>()
}
