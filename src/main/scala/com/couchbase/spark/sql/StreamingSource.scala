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

import com.couchbase.spark.sql.streaming.CouchbaseSource
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.StructType

class StreamingSource extends StreamSourceProvider {

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    ("couchbase", schema.getOrElse(CouchbaseSource.DEFAULT_SCHEMA))
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String,
                            schema: Option[StructType],
                            providerName: String, parameters: Map[String, String]): Source = {
    new CouchbaseSource(sqlContext, schema, parameters)
  }

}
