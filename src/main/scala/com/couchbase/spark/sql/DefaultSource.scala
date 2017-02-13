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

import com.couchbase.client.core.CouchbaseException
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.spark._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import com.couchbase.spark.DocumentRDDFunctions
import com.couchbase.spark.sql.streaming.{CouchbaseSink, CouchbaseSource}
import com.couchbase.spark.streaming.FromBeginning
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * The default couchbase source for Spark SQL.
 */
class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider
  with StreamSinkProvider
  with StreamSourceProvider
  with DataSourceRegister {

  override def shortName(): String = "couchbase"

  /**
   * Creates a new [[N1QLRelation]] with automatic schema inference.
   *
   * @param sqlContext the parent sql context.
   * @param parameters custom parameters that are applied.
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]):
    BaseRelation = {
    new N1QLRelation(parameters.get("bucket").orNull, None, parameters)(sqlContext)
  }

  /**
   * Creates a new [[N1QLRelation]] with a custom schema provided.
   *
   * @param sqlContext the parent sql context.
   * @param parameters custom parameters that are applied.
   * @param schema the custom schema provided by the caller.
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
    schema: StructType): BaseRelation = {
    new N1QLRelation(parameters.get("bucket").orNull, Some(schema), parameters)(sqlContext)
  }

  /**
   * Creates a [[N1QLRelation]] based on the input data and saves it to couchbase.
   *
   * @param sqlContext the parent sql context.
   * @param mode the save mode.
   * @param parameters custom parameters that are applied.
   * @param data the input data frame to store.
   */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
    parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val bucketName = parameters.get("bucket").orNull
    val idFieldName = parameters.getOrElse("idField", DefaultSource.DEFAULT_DOCUMENT_ID_FIELD)
    val removeIdField = parameters.getOrElse("removeIdField", "true").toBoolean

    val storeMode = mode match {
      case SaveMode.Append =>
        throw new UnsupportedOperationException("SaveMode.Append is not supported with Couchbase.")
      case SaveMode.ErrorIfExists => StoreMode.INSERT_AND_FAIL
      case SaveMode.Ignore => StoreMode.INSERT_AND_IGNORE
      case SaveMode.Overwrite => StoreMode.UPSERT
    }

    val datas = data
      .toJSON
      .rdd
      .map(rawJson => {
        val encoded = JsonObject.fromJson(rawJson)
        val id = encoded.get(idFieldName)
        if (id == null) {
         throw new CouchbaseException(s"Could not find ID field $idFieldName in $encoded")
        }
        if (removeIdField) {
          encoded.removeKey(idFieldName)
        }
        JsonDocument.create(id.toString, encoded)
      })
      .saveToCouchbase(bucketName, storeMode)

    createRelation(sqlContext, parameters, data.schema)
  }

  override def createSink(sqlContext: SQLContext, parameters: Map[String, String],
    partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    new CouchbaseSink(parameters)
  }

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType],
    providerName: String, parameters: Map[String, String]): (String, StructType) = {
    ("couchbase", schema.getOrElse(CouchbaseSource.DEFAULT_SCHEMA))
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String,
    schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {
    new CouchbaseSource(sqlContext, schema, parameters)
  }

}

object DefaultSource {
  val DEFAULT_STREAM_FROM: String = "BEGINNING"
  val DEFAULT_STREAM_TO: String = "INFINITY"
  val DEFAULT_DOCUMENT_ID_FIELD: String = "META_ID"
}
