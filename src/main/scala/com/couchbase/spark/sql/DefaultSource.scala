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

import com.couchbase.client.java.document.RawJsonDocument
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{CreatableRelationProvider, SchemaRelationProvider, BaseRelation, RelationProvider}
import org.apache.spark.sql.types.StructType
import com.couchbase.spark._

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
    new N1QLRelation(parameters("bucket"), None, parameters.get("schemaFilter"))(sqlContext)
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
    new N1QLRelation(parameters("bucket"), Some(schema), parameters.get("schemaFilter"))(sqlContext)
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
    val bucketName = parameters("bucket")

    data
      .toJSON
      .map(rawJson => {
        println(rawJson)
        RawJsonDocument.create("id", rawJson)
      })
      .saveToCouchbase(bucketName)

    createRelation(sqlContext, parameters, data.schema)
  }

}
