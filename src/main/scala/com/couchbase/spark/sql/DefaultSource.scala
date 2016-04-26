/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.spark.sql

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.spark._
import com.couchbase.spark.connection.{RetryOptions, CouchbaseConfig}
import org.apache.spark.Logging
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * The default couchbase source for Spark SQL.
 */
class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider {

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

    val cbConfig = CouchbaseConfig(data.sqlContext.sparkContext.getConf)

    val useRetry = parameters.getOrElse("useRetry", "false").toBoolean
    val maxRetries = if (parameters.contains("maxRetries")) parameters("maxRetries").toInt else cbConfig.retryOpts.maxTries
    val maxRetryDelay = if (parameters.contains("maxRetryDelay")) parameters("maxRetryDelay").toInt else cbConfig.retryOpts.maxDelay
    val minRetryDelay = if (parameters.contains("minRetryDelay")) parameters("minRetryDelay").toInt else cbConfig.retryOpts.minDelay
    val retryOptions = if (useRetry) RetryOptions(maxRetries, maxRetryDelay, minRetryDelay) else null


    val storeMode = mode match {
      case SaveMode.Append =>
        throw new UnsupportedOperationException("SaveMode.Append is not supported with Couchbase.")
      case SaveMode.ErrorIfExists => StoreMode.INSERT_AND_FAIL
      case SaveMode.Ignore => StoreMode.INSERT_AND_IGNORE
      case SaveMode.Overwrite => StoreMode.UPSERT
    }

    data
      .toJSON
      .map(rawJson => {
        val encoded = JsonObject.fromJson(rawJson)
        val id = encoded.getString(idFieldName)
        encoded.removeKey(idFieldName)
        JsonDocument.create(id, encoded)
      })
      .saveToCouchbase(bucketName, storeMode, retryOptions)

    createRelation(sqlContext, parameters, data.schema)
  }

}

object DefaultSource {
  val DEFAULT_DOCUMENT_ID_FIELD: String = "META_ID"
}
