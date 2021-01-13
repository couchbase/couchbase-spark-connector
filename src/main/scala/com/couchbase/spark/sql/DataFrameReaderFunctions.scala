/*
 * Copyright (c) 2021 Couchbase, Inc.
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

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader}

class DataFrameReaderFunctions(@transient val dfr: DataFrameReader) extends Serializable {

  def couchbaseQuery(explicitSchema: Option[StructType] = None,
                     options: Map[String, String] = Map[String, String]()): DataFrame = {
    val builder = dfr
      .format("com.couchbase.spark.sql.QuerySource")
      .options(options)

    explicitSchema.foreach(s => builder.schema(s))

    builder.load()
  }

}
