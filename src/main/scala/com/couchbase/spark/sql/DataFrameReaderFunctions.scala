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

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader}

class DataFrameReaderFunctions(@transient val dfr: DataFrameReader) extends Serializable {

  /**
   * The classpath to the default source (which in turn results in a N1QLRelation)
   */
  private val source = "com.couchbase.spark.sql.DefaultSource"

  /**
   * Creates a [[DataFrame]] through schema inference and no filter on the only bucket open.
   */
  def couchbase(): DataFrame = buildFrame(null, null, None)

  def couchbase(options: Map[String, String]): DataFrame =
    buildFrame(options, null, None)

  /**
   * Creates a [[DataFrame]] with a manually defined schema on the only bucket open.
   *
   * @param schema the manual schema defined.
   */
  def couchbase(schema: StructType, options: Map[String, String]): DataFrame =
    buildFrame(options, schema, None)

  def couchbase(schema: StructType): DataFrame =
    buildFrame(null, schema, None)

  /**
   * Creates a [[DataFrame]] through schema inference with a filter applied on the only bucket
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

  /**
   * Helper method to create the [[DataFrame]].
   *
   * @param schema the manual schema defined.
   * @param schemaFilter the filter clause which constraints the document set used for inference.
   */
  private def buildFrame(options: Map[String, String] = null, schema: StructType = null,
    schemaFilter: Option[Filter] = null): DataFrame = {
    val builder = dfr
      .format(source)
      .options(options)
      .option("schemaFilter", schemaFilter.map(N1QLRelation.filterToExpression).orNull)
      .schema(schema)

    if (options != null) {
      builder.options(options)
    }

    builder.load()
  }

}
