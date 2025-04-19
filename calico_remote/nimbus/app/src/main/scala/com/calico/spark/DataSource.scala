package com.calico.spark.sources.calico

import java.util

import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.connector.write._

import scala.jdk.CollectionConverters._

class DefaultSource extends TableProvider {
    override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType =
        getTable(null,Array.empty[Transform],caseInsensitiveStringMap.asCaseSensitiveMap()).schema()

  override def getTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table =
    new CalicoTable()
}


/*
  Defines Read Support and Initial Schema
 */

class CalicoTable extends Table with SupportsRead with SupportsWrite {
  // todo: SupportsDelete
  override def name(): String = this.getClass.toString

  override def schema(): StructType = StructType(Array(StructField("value", StringType)))

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new CalicoScanBuilder()

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = new CalicoWriterBuilder()
}


/*
   Scan object with no mixins

 */
class CalicoScanBuilder extends ScanBuilder {
  // todo: SupportsPushDownFilters
  // todo: SupportsPushDownRequiredColumns
  override def build(): Scan = new CalicoScan
}

/*
    Batch Reading Support
    The schema is repeated here as it can change after column pruning etc
 */

class CalicoScan extends Scan with Batch {
  // todo: SupportsReportStatistics
  override def readSchema(): StructType =  StructType(Array(StructField("value", StringType)))

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new CalicoPartition(0,4),
      new CalicoPartition(5,9))
  }
  override def createReaderFactory(): PartitionReaderFactory = new CalicoPartitionReaderFactory()
}

// simple class to organise the partition
class CalicoPartition(val start:Int, val end:Int) extends InputPartition

// reader factory
class CalicoPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new
      SimplePartitionReader(partition.asInstanceOf[CalicoPartition])
}


// parathion reader
class SimplePartitionReader(inputPartition: CalicoPartition) extends PartitionReader[InternalRow] {

  val values = Array("1", "2", "3", "4", "5","6","7","8","9","10")

  var index = inputPartition.start

  def next = index <= inputPartition.end

  def get = {
    val stringValue = values(index)
    val stringUtf = UTF8String.fromString(stringValue)
    val row = InternalRow(stringUtf)
    index = index + 1
    row
  }

  def close() = {}
}

class CalicoWriterBuilder extends WriteBuilder{
  // todo: SupportsOverwrite

  // todo: move to build()
  override def buildForBatch(): BatchWrite = new CalicoBatchWriter()
}


class CalicoBatchWriter extends BatchWrite{
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = new
    CalicoDataWriterFactory

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

class CalicoDataWriterFactory extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId:Long): DataWriter[InternalRow] = new CalicoWriter()
}



object WriteSucceeded extends WriterCommitMessage

class CalicoWriter extends DataWriter[InternalRow] {

  override def write(record: InternalRow): Unit = {
    println(record.getString(0))
  }

  override def commit(): WriterCommitMessage = WriteSucceeded

  override def abort(): Unit = {}

  override def close(): Unit = {}
}
