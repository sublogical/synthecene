use std::io;
use std::{fs::File, pin::Pin};
use std::path::Path;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use futures::Stream;
use object_store::Error as ObjectStoreError;
use tar::Archive;
use futures::stream::StreamExt;
use tokio::io::{AsyncWrite, AsyncWriteExt, AsyncReadExt};
use bytes::Bytes;

pub async fn create_archive<I: AsRef<Path>, O: AsRef<Path>>(directory: I, serialized_path: O) -> io::Result<()>{
    let tar_gz = File::create(serialized_path)?;
    let enc = GzEncoder::new(tar_gz, Compression::default());
    let mut tar = tar::Builder::new(enc);
    tar.append_dir_all("", directory)?;
    Ok(())
}

pub async fn open_archive<I: AsRef<Path>, O: AsRef<Path>>(serialized_path: I, directory: O)  -> io::Result<()>{
    let tar_gz = File::open(serialized_path)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    archive.unpack(directory)?;
    Ok(())
}


pub async fn write_multipart_file<I>(_multipart_id: String,
                                     writer: &mut Box<dyn AsyncWrite + Unpin + Send>, 
                                     path: I) -> io::Result<u64>
where
    I: AsRef<Path>
{
    let mut f = tokio::fs::File::open(path).await?;
    let mut buffer = [0u8; 10_000];
    let mut bytes_written = 0;

    loop {
        let read_bytes = f.read(&mut buffer[..]).await?;
        writer.write_all(&mut buffer[0..read_bytes]).await?;
        bytes_written += read_bytes;
        if read_bytes < 10_000 {
            break;
        }
    }
    writer.flush().await?;
    writer.shutdown().await?;

    Ok(bytes_written.try_into().expect("uploaded greater than u64?"))
}

pub async fn read_stream_file<O: AsRef<Path>>(reader: &mut Pin<Box<dyn Stream<Item = Result<Bytes, ObjectStoreError>> + Send>>, path: O) -> io::Result<()> {
    let mut f = tokio::fs::File::create(path).await?;

    while let Some(result) = reader.next().await {
        let mut bytes = result?;
        f.write_all_buf(&mut bytes).await?;
    }

    Ok(())
}



#[cfg(test)]
mod tests {
    use std::fs::{File, create_dir};
    use std::io::Write;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Instant;

    use calico_shared::result::CalicoResult;
    use object_store::ObjectStore;
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;
    use lipsum::lipsum;
    use fs_extra::dir::get_size;

    use super::*;

    fn make_file(path: &Path, size:usize) -> CalicoResult<()>{
        let mut file = File::create(path.to_str().unwrap().to_string())?;
        file.write_all(lipsum(size).as_bytes())?;
        Ok(())
    }

    #[tokio::test]
    async fn test_archive() {
        let temp_dir = tempdir().unwrap();
        let source_path = temp_dir.path().join("source");
        let num_test_files =  1_000;
        let num_test_tokens = 10_000;

        // First, lets create a source directory with a 100 files with lorem text (so compression actually happens)
        create_dir(&source_path).unwrap();
        let start = Instant::now();
        for i in 1..num_test_files+1 {
            let file_path = source_path.join(format!("subfile_{:03}", i));
            make_file(&file_path, num_test_tokens).unwrap();
        }
        let source_size = get_size(&source_path).unwrap();
        assert!(source_size > 1024 * 100);
        println!("Created {} files with size {:.2} MB in {:.2} secs", 
            num_test_files, 
            source_size as f32 / 1_000_000., 
            start.elapsed().as_millis() as f32/1000.);

        // Next, test creating an archive
        let archive_path =  temp_dir.path().join("archive.tgz");
        let start = Instant::now();
        create_archive(&source_path, &archive_path).await.unwrap();
        let archive_size = get_size(&archive_path).unwrap();
        assert!(archive_size > 5_242_880); // minimum part size
        assert!(archive_size < source_size);

        println!("Compressed {:.2} MB to {:.2} MB in {:.2} secs", 
            source_size as f32 / 1_000_000., 
            archive_size as f32 / 1_000_000.,
            start.elapsed().as_millis() as f32 / 1_000.);

        // Next, upload the archive to an object_store
        let start = Instant::now();
        let object_store_path = temp_dir.path().join("object_store");
        create_dir(&object_store_path).unwrap();
        let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&object_store_path).unwrap());
        let object_store_archive_path: object_store::path::Path = "archive.tgz".try_into().unwrap();
        let (multipart_id, mut writer) = object_store.put_multipart(&object_store_archive_path).await.unwrap();
        write_multipart_file(multipart_id, &mut writer, &archive_path).await.unwrap();
        let object_store_size = get_size(&object_store_path).unwrap();
        assert_eq!(object_store_size, archive_size);
        println!("Uploaded {:.2} MB in {} secs", 
            archive_size as f32 / 1_000_000.,
            start.elapsed().as_millis() as f32 / 1_000.);

        // Now, download from the object store
        let download_archive_path =  temp_dir.path().join("download_archive.tgz");
        let mut reader = object_store.get(&object_store_archive_path).await.unwrap().into_stream();
        read_stream_file(&mut reader, &download_archive_path).await.unwrap();
        let download_archive_size = get_size(&archive_path).unwrap();
        assert_eq!(download_archive_size, archive_size);
        println!("Downloaded {:.2} MB in {} secs", 
            download_archive_size as f32 / 1_000_000.,
            start.elapsed().as_millis() as f32 / 1_000.);

        // Finally, test opening the archive
        let start = Instant::now();
        let dest_path =  temp_dir.path().join("dest");
        open_archive(&download_archive_path, &dest_path).await.unwrap();
        let dest_size = get_size(&dest_path).unwrap();
        assert_eq!(dest_size, source_size);
        println!("Opened {} to {} in {} ms", 
            archive_size as f32 / 1_000_000.,
            dest_size as f32 / 1_000_000.,
            start.elapsed().as_millis() as f32 / 1_000.);
    }
}
