use anyhow::{Context, Result};
use log::info;
use storelib::blob::write_multipart_file;
use std::fs::{File, create_dir_all};
use std::path::Path;
use std::{path::PathBuf, sync::Arc};
use acquisition::protocol;
use arrow::array::{Array, StringArray, UInt64Array, ArrayRef, UInt8Array};
use arrow::datatypes::{Field, DataType};
use lazy_static::lazy_static;
use object_store::ObjectStore;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use uuid::Uuid;

use crate::fetch::Capture;
use crate::smarts::estimate_compression;
use crate::telemetry;

pub const CAPTURE_NORMALIZED_URL:&str = "crawler:capture:normalized_url";
pub const CAPTURE_BODY:&str =           "crawler:capture:body";
pub const CAPTURE_FETCHED_AT:&str =     "crawler:capture:fetched_at";
pub const CAPTURE_FETCH_TIME:&str =     "crawler:capture:fetch_time";
pub const CAPTURE_CONTENT_LENGTH:&str = "crawler:capture:content_length";
pub const CAPTURE_STATUS_CODE:&str =    "crawler:capture:status_code";

lazy_static! {
    pub static ref CAPTURE_SCHEMA:Arc<Schema> = Arc::new(Schema::new(vec![
        Field::new(CAPTURE_NORMALIZED_URL, DataType::Utf8, false),
        Field::new(CAPTURE_BODY, DataType::Utf8, false),
        Field::new(CAPTURE_FETCHED_AT, DataType::UInt64, false),
        Field::new(CAPTURE_FETCH_TIME, DataType::UInt64, false),
        Field::new(CAPTURE_CONTENT_LENGTH, DataType::UInt64, false),
        Field::new(CAPTURE_STATUS_CODE, DataType::UInt8, false)
    ]));
}


pub struct CaptureTelementry {    
    num_captures: telemetry::Stat,
    avg_size: telemetry::Stat,
    total_size: telemetry::Stat,
}

impl CaptureTelementry {
    pub fn init() -> CaptureTelementry {
            
        CaptureTelementry { 
            num_captures: Box::new(telemetry::SumAccumulator::default()),
            avg_size: Box::new(telemetry::MeanAccumulator::default()),
            total_size: Box::new(telemetry::SumAccumulator::default()),
        }
    }

    pub fn add(&mut self, capture: &Capture) {
        self.num_captures.add(1., 1., 0.);
        self.avg_size.add(capture.content_length as f64, 1., 0.);
        self.total_size.add(capture.content_length as f64, 1., 0.);
    }
}


fn extract_string_column<T>(data: &Vec<T>, extract: fn(&T) -> &String) -> Arc<StringArray> {
    Arc::new(StringArray::from_iter_values(data.into_iter().map(extract)))
}


fn extract_numeric_column<T, C, N>(data: &Vec<T>, extract: fn(&T) -> N) -> Arc<C> 
where C: Array + From<Vec<N>>
{
    Arc::new(C::from(data.into_iter().map(extract).collect()))
}

fn make_record_batch(data: &Vec<Capture>) -> RecordBatch {

    let normalized_url         = extract_string_column(&data, |capture| &capture.normalized_url);
    let body                   = extract_string_column(&data, |capture| &capture.body);
    let fetched_at:Arc<UInt64Array> = extract_numeric_column(&data, |capture| capture.fetched_at);
    let fetch_time:Arc<UInt64Array> = extract_numeric_column(&data, |capture| capture.fetch_time);
    let content_length:Arc<UInt64Array> = extract_numeric_column(&data, |capture| capture.content_length);
    let status_code:Arc<UInt8Array> = extract_numeric_column(&data, |capture| capture.status_code as u8);
    
    
    let table_data:Vec<ArrayRef> = vec![
        normalized_url,
        body,
        fetched_at,
        fetch_time,
        content_length,
        status_code
    ];

    RecordBatch::try_new(CAPTURE_SCHEMA.clone(),table_data).unwrap()    
}

/*
            pub body: String,

    /// Time the fetch was started, in ms since epoch
    pub fetched_at: u64,

    /// Time the fetch took to return, in ms
    pub fetch_time: u64,

    /// Content-length in bytes
    pub content_length: u64,

    pub status_code: u32,
    );

        cols.iter().map(|(name, col_data)| {
        Field::new(name, col_data.data_type().clone(), false)
    }).collect());


    let table_data:Vec<ArrayRef> = cols.iter().map(|(_, col_data)| {
        col_data.clone()
    }).collect();

    RecordBatch::try_new(Arc::new(schema),table_data).unwrap()    
*/

pub struct LocalStore {
    local_path: PathBuf,

    part_num: u32,
    part_uuid: Uuid,
    writer: Option<ArrowWriter<File>>,
    written_part_paths: Vec<PathBuf>,

    buffer: Vec<Capture>,
    max_buffer_size: usize,
    max_partfile_size: usize,

    telemetry: CaptureTelementry,
}

impl LocalStore {
    fn part_name(part_num: u32, part_uuid: Uuid) -> String {
        format!("part-{:05}-{}-c000.snappy.parquet", part_num, part_uuid)
    }

    pub async fn init<I>(_host: &protocol::Host, 
                         local_path:I) -> Result<LocalStore> 
    where
        I: AsRef<Path>,
    {
        let part_num = 0;
        let part_uuid = Uuid::new_v4();        
        let telemetry = CaptureTelementry::init();

        Ok(LocalStore {
            local_path: local_path.as_ref().to_path_buf(),
            part_num,
            part_uuid,
            writer: None,
            written_part_paths: vec![],
            buffer: vec![],
            max_buffer_size: 100,
            max_partfile_size: 128 * 1_024 * 1_024,
            telemetry,
        })
    }

    pub fn add_capture(&mut self, capture: &Capture) -> Result<()> {
        self.telemetry.add(capture);

        self.buffer.push(capture.clone());
        if self.buffer.len() >= self.max_buffer_size {
            self.flush()?;

            let estimated_partfile_size = self.telemetry.total_size.value() * estimate_compression(self.telemetry.num_captures.value(), self.telemetry.total_size.value());
            if estimated_partfile_size > self.max_partfile_size as f64 {
                self.close_writer()?;
            }
        }

        Ok(())
    }
    
    pub fn add_outlinks(&self, _outlinks: &Vec<String>) -> Result<()> {
        Ok(())
    }

    pub async fn upload(&mut self, remote_path:&PathBuf, object_store: Arc<dyn ObjectStore>) -> Result<()> {
        // closing the writer will force the latest part to be written and available for upload
        self.close_writer()?;

        self.maybe_upload(remote_path, object_store).await?;
        
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.buffer.len() == 0 {
            return Ok(());
        }

        let record_batch = make_record_batch(&self.buffer);
        let writer = self.writer()?;
        writer.write(&record_batch)?;

        self.buffer.clear();

        Ok(())
    }

    fn writer<'a>(&'a mut self) -> Result<&'a mut ArrowWriter<File>> {
        // this might be cleaner with get_or_insert_with, but that doesn't work because the closure can fail
        if self.writer.is_some() {
            return Ok(self.writer.as_mut().unwrap());
        }

        // open a file for writing
        create_dir_all(&self.local_path)?;
        let part_path = self.local_path.join(Self::part_name(self.part_num, self.part_uuid));
        let file = File::create(&part_path)
            .with_context(|| format!("Failed to create file {:?}", &part_path))?;
        
        self.writer = Some(ArrowWriter::try_new(file, CAPTURE_SCHEMA.clone(), None)
            .with_context(|| format!("Failed to create ArrowWriter for {:?}", &part_path))?);

        Ok(self.writer.as_mut().expect("Failure should have been caught above"))

    }

    fn close_writer(&mut self) -> Result<()> {
        self.flush()?;
        
        let writer = self.writer.take();
        if let Some(writer) = writer {
            writer.close()?;

            self.written_part_paths.push(self.local_path.join(Self::part_name(self.part_num, self.part_uuid)));

            self.part_num += 1;
            self.telemetry = CaptureTelementry::init();
        }

        Ok(())
    }

    pub async fn maybe_upload(&mut self, remote_path:&PathBuf, object_store: Arc<dyn ObjectStore>) -> Result<()> {
        for input_path in self.written_part_paths.iter() {
            let object_path = remote_path.join(input_path.file_name().unwrap());
            let object_path: object_store::path::Path = object_path.to_string_lossy().to_string().try_into().unwrap();

            let (multipart_id, mut writer) = object_store.put_multipart(&object_path).await?;
            let bytes_written = write_multipart_file(multipart_id, &mut writer, input_path).await?;

            info!("Wrote Capture: {} bytes to {}", bytes_written, object_path);
        }
        
        self.written_part_paths = vec![];

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use acquisition::protocol;
    use fs_extra::dir::get_size;
    use object_store::{ObjectStore, local::LocalFileSystem};
    use rand::{thread_rng, distributions::Alphanumeric, Rng};
    use tempfile::tempdir;

    use crate::fetch::Capture;

    use super::LocalStore;

    fn make_capture(body_size: usize) -> Capture {
        let body: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(body_size)
            .map(char::from)
            .collect();

        let normalized_url: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(100)
            .map(char::from)
            .collect();

        Capture {
            normalized_url,
            body,
            fetched_at: 0,
            fetch_time: 0,
            content_length: body_size.try_into().unwrap(),
            status_code: 200,
            inlinks: vec![],
            outlinks: vec![],
        }
    }
    #[tokio::test]
    async fn upload() {
        let remote_temp_dir = tempdir().unwrap();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(remote_temp_dir.path()).unwrap());

        let local_temp_dir = tempdir().unwrap();

        let mut store = LocalStore::init(&protocol::Host::default(), local_temp_dir.path()).await.unwrap();
        for _ in 0..10 {
            store.add_capture(&make_capture(1_000)).unwrap();
        }

        store.upload(&"remote".to_string().into(), object_store).await.unwrap();

        let folder_size = get_size(remote_temp_dir.path()).unwrap();
        assert!(folder_size > 0);   
    }
}
