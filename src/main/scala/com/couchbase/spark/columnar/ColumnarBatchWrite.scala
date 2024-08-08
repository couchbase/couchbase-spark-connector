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

import com.couchbase.spark.config.CouchbaseConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

class ColumnarBatchWrite(info: LogicalWriteInfo,
                         schema: StructType,
                         writeConfig: ColumnarWriteConfig,
                        ) extends BatchWrite with Logging {
  private lazy val conf =
    CouchbaseConfig(SparkSession.active.sparkContext.getConf, writeConfig.connectionIdentifier)


  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new ColumnarDataWriterFactory(schema, writeConfig, conf)
  }

  // Called once all data are written successfully.  The messages are the output from ColumnarDataWriter.commit().
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logDebug(s"Commit write for query ${info.queryId} from ${messages.size} messages")
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logDebug(s"Abort writing for query ${info.queryId} from ${messages.size} messages")
  }
}
