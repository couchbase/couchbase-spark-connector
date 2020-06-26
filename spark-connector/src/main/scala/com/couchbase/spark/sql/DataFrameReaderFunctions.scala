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

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader}

class DataFrameReaderFunctions(@transient val dfr: DataFrameReader) extends Serializable {

  /**
   * The classpath to the default source (which in turn results in a N1QLRelation)
   */
  private val source = "com.couchbase.spark.sql.DefaultSource"

  /**
   * Creates a DataFrame through schema inference and no filter on the only bucket open.
   */
  def couchbase(): DataFrame = buildFrame(null, null, None)

  def couchbase(options: Map[String, String]): DataFrame =
    buildFrame(options, null, None)

  /**
   * Creates a DataFrame with a manually defined schema on the only bucket open.
   *
   * @param schema the manual schema defined.
   */
  def couchbase(schema: StructType, options: Map[String, String]): DataFrame =
    buildFrame(options, schema, None)

  def couchbase(schema: StructType): DataFrame =
    buildFrame(null, schema, None)

  /**
   * Creates a DataFrame through schema inference with a filter applied on the only bucket
   * open.
   *
   * The filter will be essentially turned into a WHERE clause on the N1QL query, which allows
   * for much more exact schema inference, especially on a large and/or diverse data set.
   *
   * @param schemaFilter the filter clause which constraints the document set used for inference.
   */
  def couchbase(schemaFilter: Filter, options: Map[String, String]): DataFrame =
    buildFrame(options, null, Some(schemaFilter))

  def couchbase(schemaFilter: Filter): DataFrame =
    buildFrame(null, null, Some(schemaFilter))

  def couchbase(schema: StructType, schemaFilter: Filter): DataFrame =
    buildFrame(null, schema, Some(schemaFilter))

  def couchbase(schema: StructType, schemaFilter: Filter, options: Map[String, String]): DataFrame =
    buildFrame(options, schema, Some(schemaFilter))

  /**
   * Helper method to create the DataFrame.
   *
   * @param schema the manual schema defined.
   * @param schemaFilter the filter clause which constraints the document set used for inference.
   */
  private def buildFrame(options: Map[String, String] = null, schema: StructType = null,
    schemaFilter: Option[Filter] = null): DataFrame = {
    val builder = dfr
      .format(source)
      .schema(schema)

    val filter = schemaFilter.map(N1QLRelation.filterToExpression)
    if (filter.isDefined) {
      builder.option("schemaFilter", filter.get)
    }

    if (options != null) {
      builder.options(options)
    }

    builder.load()
  }

}
