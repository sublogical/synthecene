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

## Transaction Log

UPDATE
(OBJ)   [id]    [column-a]  [column-a-op]   (column-a-view)
001     001     "a"         set             "a"
002     001     "b"         append          "a", "b"
003     001     "c"         append          "a", "b", "c"
004     001     "d"         set             "d"
005     001     null        delete          null
006     001     "a"         append          "a"
007     001     "a"         set             "a"

{ action: append-record, object="001" version="1" }
{ action: append-record, object="002" version="2" }
{ action: append-record, object="003" version="3" }
{ action: append-record, object="004" version="4" }
{ action: append-record, object="005" version="5" }
{ action: checkpoint,    object="006" version="5" }



## TODO List
* Independence from Spark
* Tile-Stats
  * Support automatica tile-level statistical aggregations, defined in SQL
  * Support optimization of analytical SQL queries using tile-stats
* Streaming Writes
  * Support streaming write service
  * Support streaming writes via kafka topic
* Transactions
  * Support transaction log tree
  * Support branch labels (e.g. mainline)
  * Support operations with set of parquet files + update expression
  * Support parallel checkpoint operations
  * Support squash + rebase to combine lots of small updates to a single update
  * Support vaccuum / garbage collection on pre-checkpoint commits
  * Support vaccuum / garbage collection on unreferenced objects
  * Support local-file db transaction log
  * Support remote-file db 
  * Support dynamo db transaction log
* Storage Interfaces
  * Support local-file storage
  * Support s3 storage
  * Support remote-file (sshfs/scp) storage
  * Support hdfs storage
  * Support lustr storage
  * Support dynamo db storage
  * Support in-memory storage (for pre-commit read consistency)
* Partitioning
  * ~~Support column-group based partition definition~~
  * ~~Support hash-based partitions~~
  * Support time-series partition scheme
  * Support multipartition (e.g. time series + hash)
  * ~~Support partitioning on one or more components of key-space~~
  * ~~Support column partitioning~~
* Writers
  * Support direct multi-partition writes from Rust
  * Support direct multi-partition writes from Python
  * Support direct multi-partition writes from Java/Scala
  * Support writes from Flink
  * Support writes from Spark
* Readers
  * Integrate with DataFusion for reads
  * Support reading a tile from Checkpoint + Vec<Update>
  * Support reading a dataset from multiple partitions
  * Support reading a dataset from multiple column-groups
  * Support direct reader for Rust
  * Support direct reader for Python
  * Support direct reader for Java/Scala
  * Support distributed reads from Spark
  * Support distributed reads from Flink
  * Support time-travel reads
  * Support denormalized reads
* Indexing
  * Support inverted index reads on text columns
  * Support structured indexes
* Stats
  * Support HLL sketches
 
 
  





