use std::fs::File;
use std::sync::Arc;

use arrow::{datatypes::Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::partition::{ColumnGroupRef, ColumnRef };


// todo: move to protobuf or whatever
pub struct _StorageConfig {
    pub column: ColumnRef,
    pub column_group: ColumnGroupRef
}

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


// TODO: append transaction
// TODO: delete transaction
// TODO: compact/snapshot operation
// TODO: garbage collection


