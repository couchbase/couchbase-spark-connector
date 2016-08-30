/*
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.spark.sql

import org.apache.spark.sql.DataFrameWriter

class DataFrameWriterFunctions(@transient val dfw: DataFrameWriter[AnyRef]) extends Serializable {

  /**
   * The classpath to the default source (which in turn results in a N1QLRelation)
   */
  private val source = "com.couchbase.spark.sql.DefaultSource"

  /**
   * Stores the current DataFrame in the only open bucket.
   */
  def couchbase(options: Map[String, String] = null): Unit = writeFrame(options)

  /**
   * Helper method to write the current DataFrame against the couchbase source.
   */
  private def writeFrame(options: Map[String, String] = null): Unit = {
    val builder = dfw
      .format(source)

    if (options != null) {
      builder.options(options)
    }

    builder.save()
  }

}
