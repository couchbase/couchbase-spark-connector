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

import com.couchbase.spark.core.CouchbaseConnection
import com.couchbase.spark.sql.QuerySource.DEFAULT_ID_FIELD_NAME
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class QuerySource
  extends RelationProvider
    with SchemaRelationProvider
    with DataSourceRegister {

  override def shortName(): String = "couchbaseQuery"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    new QueryRelation(
      sqlContext.sparkContext.broadcast(CouchbaseConnection()),
      argsToOptions(parameters, None)
    )(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    new QueryRelation(
      sqlContext.sparkContext.broadcast(CouchbaseConnection()),
      argsToOptions(parameters, Some(schema))
    )(sqlContext)
  }

  private def argsToOptions(parameters: Map[String, String],
                            schema: Option[StructType]): CouchbaseSqlQueryOptions = {
    val couchbaseParameters = parameters.filter(p => p._1.startsWith("couchbase"))
    val otherParameters = parameters.filter(p => !p._1.startsWith("couchbase"))

    CouchbaseSqlQueryOptions(
      explicitSchema = schema,
      schemaFilter = couchbaseParameters.get("couchbase.schemafilter"),
      schemaOptions = otherParameters,
      bucketName = couchbaseParameters.get("couchbase.bucket"),
      idFieldName = couchbaseParameters.getOrElse("couchbase.idfieldname", DEFAULT_ID_FIELD_NAME)
    )
  }

}

object QuerySource {
  val DEFAULT_ID_FIELD_NAME = "META_ID"
}
