/*
 * Copyright (c) 2017 Couchbase, Inc.
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
package com.couchbase.spark.sql.streaming

import com.couchbase.spark.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.execution.streaming.Sink
import com.couchbase.spark.sql._

/**
  * A simple Structured Streaming sink which writes the data frame to the bucket.
  *
  * Note that for now Upsert is always used internally to not run into document already
  * exists exception, especially when writing aggregates.
  *
  * @param options options passed from the upper level down to the dataframe writer.
  */
class CouchbaseSink(options: Map[String, String]) extends Sink with Logging {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    data.write.mode(SaveMode.Overwrite).couchbase(options)
  }

}
