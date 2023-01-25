use std::sync::Arc;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use calico_shared::result::CalicoResult;
use clap::{Parser, Args, Subcommand};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use futures::{TryStreamExt};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use pretty_bytes::converter::convert;
use storelib::log::ReferencePoint;
use storelib::table::{Table, OBJECT_PATH, TableStore};
use storelib::{test_util::*, protocol};
use storelib::operation::append::append_operation;
use tempfile::{tempdir};
use tokio::time::Instant;
use url::Url;

async fn get_object_store_size(object_store: &Arc<dyn ObjectStore>) -> u64 {
    let prefix: ObjectStorePath = OBJECT_PATH.try_into().unwrap();

    let paths = object_store.list(Some(&prefix))
        .await.unwrap()
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<ObjectStorePath>>().await.unwrap();

    let mut sum = 0;
    for path in paths {
        sum += object_store.head(&path).await.unwrap().size;
    }

    sum.try_into().unwrap()
}

async fn run_append_load(table_store: Arc<TableStore>, 
                         num_rows:usize, 
                         string_size: usize, 
                         columns: &Vec<String>, 
                         ids: Arc<dyn Array>) -> CalicoResult<protocol::Commit>{
    let mut cols = vec![(ID_FIELD, ids)];

    for col in columns {
        let big_strs = big_str_col(string_size,num_rows);
        let big_strs:Vec<&str> = big_strs.iter().map(AsRef::as_ref).collect();
        cols.push((col, str_col(&big_strs)));
    }

    let table = build_table(&cols);

    Ok(append_operation(table_store, table).await?)
}

async fn perform_query(table_store: Arc<TableStore>, 
                       ctx: &SessionContext,
                       columns: &Vec<String>) -> CalicoResult<Vec<RecordBatch>> {
    let reference = ReferencePoint::Main;
    let columns: Vec<&str> = columns.iter().map(AsRef::as_ref).collect();
    let table_schema = make_schema(&columns);
    let sql = format!("SELECT * FROM test LIMIT 10");
    
    let table = Table::define(table_store, table_schema, reference).unwrap();    
    ctx.register_table("test", table).unwrap();

    let df = ctx.sql(sql.as_str()).await.unwrap();
    
    let actual: Vec<RecordBatch> = df.collect().await.unwrap();

    Ok(actual)
}

pub fn provision_ctx(path: &std::path::Path) -> SessionContext {
    let ctx = SessionContext::new();
    let object_store:Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(path).unwrap());

    ctx.state
        .try_write().unwrap().runtime_env.register_object_store("file", "temp", object_store);
            
    ctx
}


/// calico-loadgen --object file://location
/// calico-loadgen --object temp://

#[derive(Parser, Debug)]
struct LoadGenArgs {
   /// The URL of the object store to be loaded. Currently accepted object store types include:
   /// -- file://{local-file-path}   
   /// 
   #[arg(short, long)]
   object: Option<String>,

   /// Indicates that the contents of the object store should be cleaned prior to usage
   #[arg(short, long, default_value_t = false)]
   reset: bool,

   // specify number of column groups to simulate
   #[arg(long, default_value_t = 1)]
   num_column_groups: u32,

   // specify number of columns to simulate
   #[arg(long, default_value_t = 4)]
   num_columns: u32,

   // specify ID fields to use
   #[arg(long, default_value = "id")]
   id_fields: Vec<String>,

   // specify number of partitions to use
   #[arg(long, default_value_t = 2)]
   num_partitions: u64,

   #[command(subcommand)]
   command: Command

}

#[derive(Args, Debug)]
struct AppendArgs {
    #[arg(long, default_value_t = 1)]
    num_workers: u32,

    #[arg(long, default_value_t = 100)]
    num_commits: usize,

    #[arg(long, default_value_t = 1000)]
    num_rows: usize,

    #[arg(long, default_value_t = 100)]
    string_size: usize,

   /// Skip the read at the end
   #[arg(long, default_value_t = false)]
   skip_read: bool,
}
#[derive(Subcommand, Debug)]
enum Command {
    Append(AppendArgs)
}

impl LoadGenArgs {
    fn to_colgroups(&self) -> Vec<String> {
        (0..self.num_column_groups).map(|idx| {
            format!("cg{}", idx)
        }).collect()
    }

    fn to_columns(&self) -> Vec<String> {
        (0..self.num_columns).map(|idx| {
            format!("field_{}", idx)
        }).collect()
    }


    fn initialize_object_store(&self, ctx: &SessionContext, path: &std::path::Path) -> CalicoResult<Arc<dyn ObjectStore>> {
        let object_store = match &self.object {
            Some(path) => match Url::parse(path) {
                Ok(url) => match url.scheme() {
                    "file" => {
                        let path = url.path();
                        println!("Using LocalFile at: {}", path);
                        Arc::new(LocalFileSystem::new_with_prefix(path).unwrap())
                    }
                    "s3" => todo!("support S3"),
                    scheme => panic!("Unknown object scheme {}", scheme)
                },
                Err(_) => {
                    println!("Using LocalFile at: {}", path);
                    Arc::new(LocalFileSystem::new_with_prefix(path).unwrap())
                },
            },
            None => {
                println!("Using LocalFile(TMP) at: {}", path.to_str().unwrap());
                Arc::new(LocalFileSystem::new_with_prefix(path).unwrap())
            }
        };

        // todo: support loading whatever object store
        ctx.state
            .try_write().unwrap().runtime_env.register_object_store("file", "loadgen", object_store.clone());

        Ok(object_store)
    }

    async fn provision_table(&self, object_store: Arc<dyn ObjectStore>) -> CalicoResult<Arc<TableStore>> {
        let object_store_url = ObjectStoreUrl::parse("file://loadgen").unwrap();
        let mut table_store:TableStore = TableStore::new(object_store_url, object_store).await.unwrap();
        let column_groups = self.to_colgroups();
        let columns = self.to_columns();

        // todo: support reset/init

        for column_group in &column_groups {
            table_store.add_column_group(protocol::ColumnGroupMetadata { 
                column_group: column_group.to_string(),
                id_columns: vec![ID_FIELD.to_string()],
                partition_spec: Some(protocol::column_group_metadata::PartitionSpec::KeyHash(
                    protocol::KeyHashPartition { 
                        partition_keys: self.id_fields.clone(), 
                        num_partitions: self.num_partitions })
                )}).await?;
        }
        
        for (index, column) in columns.iter().enumerate() {
            let column_group = column_groups[index % self.num_column_groups as usize].clone();

            table_store.add_column(protocol::ColumnMetadata { 
                column: column.to_string(), 
                column_group: column_group.to_string() 
            }).await?;
        }

        Ok(Arc::new(table_store))
    }
}

async fn append_loadgen(task: &AppendArgs, ctx: &SessionContext, table_store: Arc<TableStore>, columns: Vec<String>) -> CalicoResult<()> {
    let ids = u64_col(&big_col(task.num_rows));

    let object_store = table_store.default_object_store().await.unwrap();
    let start_size = get_object_store_size(&object_store).await;
    let start = Instant::now();

    // todo: support multiple workers
    for _ in 0..task.num_commits {
        run_append_load(table_store.clone(), task.num_rows, task.string_size, &columns, ids.clone()).await.unwrap();
    }

    let duration = start.elapsed();
    println!("Completed write in: {:?} ({:.2} commits/sec, {:.2} rows/sec)", 
        duration, 
        task.num_commits as f32 / duration.as_secs_f32(),
        (task.num_commits * task.num_rows) as f32 / duration.as_secs_f32());

    let end_size = get_object_store_size(&object_store).await;
    let delta_size = end_size - start_size;

    println!("Delta Size: {} ({} bytes/sec)", 
        convert(delta_size as f64),
        convert(delta_size as f64 /  duration.as_secs_f64()));

    if !task.skip_read {
        let start = Instant::now();
        perform_query(table_store.clone(), &ctx, &columns).await.unwrap();
        let duration = start.elapsed();
        println!("Completed query in: {:?}", duration);
    }

    // todo: output stats
    Ok(())
}

#[tokio::main]
async fn main() -> CalicoResult<()>{

    let temp = tempdir().unwrap();
    let args = LoadGenArgs::parse();
    let ctx = SessionContext::new();
    let object_store = args.initialize_object_store(&ctx, &temp.path())?;
    let table_store = args.provision_table(object_store).await?;
    let columns = args.to_columns();

    let _result = match &args.command {
        Command::Append(task) => append_loadgen(task, &ctx, table_store, columns).await?
    };

    Ok(())
}
