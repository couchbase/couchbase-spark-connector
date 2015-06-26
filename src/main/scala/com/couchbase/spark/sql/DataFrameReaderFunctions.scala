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

  /**
   * Creates a [[DataFrame]] through schema inference and no filter on a specific open bucket.
   *
   * @param bucket the name of the bucket to use.
   */
  def couchbase(bucket: String): DataFrame = buildFrame(bucket, null, None)

  /**
   * Creates a [[DataFrame]] with a manually defined schema on the only bucket open.
   *
   * @param schema the manual schema defined.
   */
  def couchbase(schema: StructType) = buildFrame(null, schema, None)

  /**
   * Creates a [[DataFrame]] with a manually defined schema on a specific open bucket.
   *
   * @param schema the manual schema defined.
   * @param bucket the name of the bucket to use.
   */
  def couchbase(schema: StructType, bucket: String) = buildFrame(bucket, schema, None)

  /**
   * Creates a [[DataFrame]] through schema inference with a filter applied on the only bucket
   * open.
   *
   * The filter will be essentially turned into a WHERE clause on the N1QL query, which allows
   * for much more exact schema inference, especially on a large and/or diverse data set.
   *
   * @param schemaFilter the filter clause which constraints the document set used for inference.
   */
  def couchbase(schemaFilter: Filter) = buildFrame(null, null, Some(schemaFilter))

  /**
   * Creates a [[DataFrame]] through schema inference with a filter applied on a specific open
   * bucket.
   *
   * The filter will be essentially turned into a WHERE clause on the N1QL query, which allows
   * for much more exact schema inference, especially on a large and/or diverse data set.
   *
   * @param schemaFilter the filter clause which constraints the document set used for inference.
   * @param bucket the name of the bucket to use.
   */
  def couchbase(schemaFilter: Filter, bucket: String) = buildFrame(bucket, null, Some(schemaFilter))

  private def buildFrame(bucket: String = null, schema: StructType = null,
    schemaFilter: Option[Filter] = null) = {
    dfr
      .format(source)
      .option("bucket", bucket)
      .option("schemaFilter", schemaFilter.map(N1QLRelation.filterToExpression).orNull)
      .schema(schema)
      .load()
  }

}
