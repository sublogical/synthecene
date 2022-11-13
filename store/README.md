# Calico Storage System
## Introduction
The storage system for calico lobster is a distributed ACID column-store with 
support for distributed reads and writes using any distributed compute 
framework such as Spark, or Flink, or using adhoc distributed compute. The 
system supports partitioning, transactional updates, upserts, time-travel, time-aware joins, denormalized multi-dimensional data, indexing, 
statistical aggregation, pushdown query and transformation expressions, access 
control at a column-group and row level, and multiple key-spaces with optimized
cross-keyspace joins and materialization. Data can be efficiently streamed in 
and out of the store by any number of independent writers. Column-groups can be 
independently managed on all dimensions of configuration.

Physical storage can be file-based, object-based or table-based, and can leverage 
any of the cloud storage providers.

## Data Model
```
/{column-group}/{column}
```

* column-group level configuration
  * transaction log level (global, column-group, partition, column or tile)


## Transaction Log

```
UPDATE
(OBJ)   [id]    [column-a]  [column-a-op]   (column-a-view)
001     001     "a"         set             "a"
002     001     "b"         append          "a", "b"
003     001     "c"         append          "a", "b", "c"
004     001     "d"         set             "d"
005     001     null        delete          null
006     001     "a"         append          "a"
007     001     "a"         set             "a"
```

* transactions are performed at a column-group level, or 
* checkpoints are performed at a column-group level



```
{ action: append-record, object="001" version="1" }
{ action: append-record, object="002" version="2" }
{ action: append-record, object="003" version="3" }
{ action: append-record, object="004" version="4" }
{ action: append-record, object="005" version="5" }
{ action: checkpoint,    object="006" version="5" }
```




## TODO List
* Reader API
  * Support reading a dataset from checkpoint
  * Support reading a dataset from multiple column-groups
  * Support time-travel reads
  * Support direct reader for Python
  * Support direct reader for Java/Scala
  * Support disaggregated reads from Rust (Ballista?)
  * Support disaggregated reads from Spark
  * Support disaggregated reads from Flink
  * Support denormalized reads
  * ~~Support reading a dataset from multiple commits~~
  * ~~Support reading a dataset from multiple partitions~~
  * ~~Integrate with DataFusion for reads~~
  * ~~Support reading a dataset from single partition, commit & column-group~~
  * ~~Support direct reader for Rust~~
* Cleanup
  * Move multizip out to a utility class
* DataFusion
  * Add BinaryType to TryFrom<&DataType> for ScalarValue
  * Add BinaryType to MergeSortJoin (line 1112)
  * Implement statistics() for SortMergeJoinExec
* Schema
  * ~~Make ID column explicit part of column-group definition~~
  * Support more int-based IDs
  * Support string based IDs
  * Support multiple IDs
  * Sensible errors for ID failures
  * Move partitioning to use discrete values of a set of partition columns
  * Read schema from transaction log
* Operator API
  * ~~Move to Command Pattern~~
  * Support streaming write
  * Refactor operator pattern for disaggregated writes
  * DDL Operations
  * Direct Checkpoint
  * Garbage Collect Data
    * Unreferenced Data Objects
    * Old Tmp Objects
    * Pre-Checkpoint Commits
    * Aged-Out Checkpoints
  * Disaggregated Write from Rust
  * Disaggregated Write from Spark
  * Disaggregated Write from Flink
  * Disaggregated Write from Beam
  * Disaggregated Checkpoint from Rust
  * Write with Operator
  * Direct Squash from Rust
  * Disaggregated Squash from Rust
  * ~~Direct Write from Rust~~
* Transaction Log
  * Support ReferencePoint Parser
  * Support Column & ColumnGroup metadata in transaction log
  * Support squash + rebase to combine lots of small updates to a single update
  * Support vaccuum / garbage collection on pre-checkpoint commits
  * Support vaccuum / garbage collection on unreferenced objects
  * ~~Support ReferencePoint~~
  * ~~Support transaction log tree~~
  * ~~Support branch labels (e.g. mainline)~~
  * ~~Support operations with set of parquet files + update expression~~
  * ~~Support parallel checkpoint operations~~
* Benchmark
  * Transaction Log
    * Commit Speed
    * Lookup Speed
  * Operations
    * Append
    * Checkpoint
  * Data Generator
  * Reader
* CLI
  * Create Table
  * Add Column-Group
    * From params
    * From args
  * Add Column
    * From params
    * From args
  * Add data
    * From CSV
    * From JSON
    * From Parquet
  * Read data
    * dump table
    * timetravel view
    * to CSV
    * to JSON
    * to parquet
* ObjectStore Interfaces
  * Support dynamo db storage
  * Support redis storage
  * Support remote-file (sshfs/scp) storage
  * Support hdfs storage
  * Support lustr storage
  * ~~Support local-file storage~~
  * ~~Support s3 storage~~ (untested)
  * ~~Support in-memory storage (for pre-commit read consistency)~~ (untested)
* Partitioning
  * Support explicit column-based partitions
  * Support multi-column partitions
  * Support bucketing/clustering
  * Support z-order
  * ~~Support all integer numeric partitions~~
  * ~~Support string partitionings~~
  * ~~Support column-group based partition definition~~
  * ~~Support hash-based partitions~~
  * ~~Support partitioning on one or more components of key-space~~
  * ~~Support column partitioning~~
* Write APIs
  * Support direct multi-partition writes from Python
  * Support direct multi-partition writes from Java/Scala
  * Support writes from Flink
  * Support writes from Spark
  * ~~Support direct multi-partition writes from Rust~~
* Tile-Stats
  * Support Datafusion Stats
  * Support HLL sketches
  * Support KLL sketches
  * Support automatic tile-level statistical aggregations, defined in SQL
  * Support optimization of analytical SQL queries using tile-stats
* Streaming Writes
  * Support streaming partitioning
  * Support streaming compaction
  * Support streaming write service (arrow flight, grpc)
  * Support streaming writes via kafka topic
  * Preserve kafka commits while in-flight
  * Support in-memory queries on uncommitted transactions
* Indexing
  * Support inverted index reads on text columns
  * Support structured indexes
  
* Immediate
  * ~~Move all Files in a commit to a single transaction~~
  * ~~Include tile in commits & checkpoints~~
  * ~~Implement Commit|Checkpoint to Table (Vec<Vec<PartitionedFile>>)~~
 
  





