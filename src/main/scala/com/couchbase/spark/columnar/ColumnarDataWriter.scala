/*
 * Copyright (c) 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.spark.columnar

import com.couchbase.client.scala.analytics.{AnalyticsOptions => CouchbaseAnalyticsOptions}
import com.couchbase.spark.columnar.ColumnarConstants.ColumnarEndpointIdx
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.json.{JSONOptions, JSONOptionsInRead, JacksonGenerator}
import org.apache.spark.sql.internal.SQLConf

import java.io.StringWriter
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

case class ColumnarWriterCommitMessage() extends WriterCommitMessage

// Responsible for writing a partition of data to a data source.
// Run entirely on the executors.
class ColumnarDataWriter(
    val schema: StructType,
    val writeConfig: ColumnarWriteConfig,
    val conf: CouchbaseConfig,
    val partitionId: Int,
    val taskId: Long
) extends DataWriter[InternalRow]
    with Logging {

  // Per docs, no thread-safety concerns to worry about.
  private val batch = ArrayBuffer.empty[InternalRow]

  override def write(record: InternalRow): Unit = {
    batch += record.copy()
  }

  override def commit(): WriterCommitMessage = {
    val writer = new StringWriter()
    val opts = new JSONOptionsInRead(Map.empty, SQLConf.get.sessionLocalTimeZone)
    val jg = new JacksonGenerator(schema, writer, opts)

    for (i <- Range(0, batch.size)) {
      jg.write(batch(i))
      jg.flush()
      if (i != batch.size - 1) {
        writer.write(',')
      }
    }

    val sql =
      s"INSERT INTO `${writeConfig.database}`.`${writeConfig.scope}`.`${writeConfig.collection}` [${writer.toString}]"

    logInfo(s"Executing Columnar write query $sql")

    CouchbaseConnection(writeConfig.connectionIdentifier)
      .cluster(conf)
      .analyticsQuery(sql, buildOptions())

    ColumnarWriterCommitMessage()
  }

  private def quoteIfNeeded(s: Any): Boolean = {
    // A mix of what Spark documents it returns - and what we've empirically found it does return.
    s match {
      case _: org.apache.spark.unsafe.types.UTF8String => true
      case _: String                                   => true
      case _: java.sql.Date                            => true
      case _: java.time.LocalDate                      => true
      case _: java.sql.Timestamp                       => true
      case _: java.time.Instant                        => true
      case _                                           => false
    }
  }

  def buildOptions(): CouchbaseAnalyticsOptions = {
    var opts = CouchbaseAnalyticsOptions().endpointIdx(ColumnarEndpointIdx)
    writeConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }

  override def abort(): Unit = {
    logInfo(s"partition=${partitionId} task=${taskId} Aborting")
  }

  override def close(): Unit = {
    logInfo(s"partition=${partitionId} task=${taskId} Closing")
  }
}
