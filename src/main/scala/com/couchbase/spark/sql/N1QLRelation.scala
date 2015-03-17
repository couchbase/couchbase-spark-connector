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

import com.couchbase.client.java.query.Query
import com.couchbase.spark.connection.CouchbaseConfig
import com.couchbase.spark.rdd.QueryRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{EqualTo, Filter, PrunedFilteredScan, BaseRelation}
import org.apache.spark.sql.types._

class N1QLRelation(bucket: String, userSchema: Option[StructType])(@transient val sqlContext: SQLContext) extends BaseRelation with PrunedFilteredScan {

  private val cbConfig = CouchbaseConfig(sqlContext.sparkContext.getConf)

  val bucketName = if (bucket == null) {
    cbConfig.buckets(0).name
  } else {
    bucket
  }

  override val schema = userSchema.get

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val query = "SELECT " + buildColumns(requiredColumns) + " FROM `" + bucketName + "`" + buildFilter(filters)
    val usableSchema = schema

    QueryRDD(sqlContext.sparkContext, bucketName, Query.simple(query)).map(row => {
      val mapped = requiredColumns.map(column => {
        val neededType = usableSchema(column).dataType
        
        if (neededType == StringType) {
          row.value.getString(column)
        } else if (neededType == FloatType) {
          row.value.getDouble(column).toFloat
        } else if (neededType == DoubleType) {
          row.value.getDouble(column).asInstanceOf[Double]
        } else {
          throw new Exception("Unhandled type" + neededType)
        }
      })

      Row.fromSeq(mapped.toSeq)
    })
  }

  private def buildColumns(requiredColumns: Array[String]): String =  {
    requiredColumns.mkString(",")
  }

  private def buildFilter(filters: Array[Filter]): String = {
    var filter = ""
    if (filters.length > 0) {
      filter = " WHERE"
      filters.foreach {
        case EqualTo(attr, value) => filter = filter + " " + attr + " = " + "\"" + value + "\""
        case _ => throw new Exception ("Unsupported filter")
      }
    }
    filter
  }

}
