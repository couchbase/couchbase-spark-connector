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

import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.query.Query
import com.couchbase.spark.connection.CouchbaseConfig
import com.couchbase.spark.rdd.QueryRDD
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._

import scala.collection.JavaConversions._

/**
 * Implements a the BaseRelation for N1QL Queries.
 *
 * TODO:
 *  - fix where clause recursions and operators
 *  - recursive stuff in buildScan
 *
 * @param bucket the name of the bucket
 * @param userSchema the optional schema (if not provided it will be inferred)
 * @param sqlContext the sql context.
 */
class N1QLRelation(bucket: String, userSchema: Option[StructType], filter: Option[Filter])
                  (@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan
  with Logging {

  private val cbConfig = CouchbaseConfig(sqlContext.sparkContext.getConf)
  private val bucketName = Option(bucket).getOrElse(cbConfig.buckets(0).name)

  override val schema = userSchema.getOrElse[StructType] {

    val queryFilter = if (filter.isDefined) {
      buildFilter(Array(filter.get))
    } else {
      ""
    }

    val query = s"SELECT `$bucketName`.* FROM `$bucketName` $queryFilter LIMIT 100"
    logInfo(s"Inferring schema from bucket $bucketName with query '$query'")

    sqlContext.jsonRDD(
      QueryRDD(sqlContext.sparkContext, bucketName, Query.simple(query)).map(_.value.toString)
    ).schema
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val mergedFilters = if (filter.isDefined) {
      filters ++ filter
    } else {
      filters
    }

    val query = "SELECT " + buildColumns(requiredColumns) + " FROM `" + bucketName + "`" + buildFilter(mergedFilters)
    val usableSchema = schema

    logInfo(s"Executing generated query: '$query'")
    QueryRDD(sqlContext.sparkContext, bucketName, Query.simple(query)).map(row => {
      val mapped = requiredColumns.map(column => {
        val neededType = usableSchema(column).dataType

        neededType match {
          case StringType => row.value.getString(column)
          case FloatType => row.value.getDouble(column).toFloat
          case DoubleType => row.value.getDouble(column).toDouble
          case BooleanType => row.value.getBoolean(column)
          case IntegerType => row.value.getInt(column).toInt
          case LongType => row.value.getLong(column).toLong
          case _ => throw new Exception("Unhandled Type: " + neededType)
        }
      })

      Row.fromSeq(mapped.toSeq)
    })
  }

  /**
   * Transforms the required columns into the field list for the select statement.
   *
   * @param requiredColumns the columns to transform.
   * @return the raw N1QL string
   */
  private def buildColumns(requiredColumns: Array[String]): String =  {
    requiredColumns.map(column => "`" + column + "`").mkString(",")
  }


  /**
   * Transform the filters into a N1QL where clause.
   *
   * @todo In, And, Or, Not filters including recursion
   * @param filters the filters to transform
   * @return the transformed raw N1QL clause
   */
  private def buildFilter(filters: Array[Filter]): String = {
    if (filters.isEmpty) {
      return ""
    }

    val filter = new StringBuilder(" WHERE")
    var i = 0

    filters.foreach(f => {
      if (i > 0) {
        filter.append(" AND")
      }

      f match {
        case EqualTo(attr, value) => filter.append(s" `$attr` = '$value'")
        case GreaterThan(attr, value) => filter.append(s" `$attr` > $value")
        case GreaterThanOrEqual(attr, value) => filter.append(s" `$attr` >= $value")
        case LessThan(attr, value) => filter.append(s" `$attr` < $value")
        case LessThanOrEqual(attr, value) => filter.append(s" `$attr` <= $value")
        case IsNull(attr) => filter.append(s" `$attr` IS NULL")
        case IsNotNull(attr) => filter.append(s" `$attr` IS NOT NULL")
        case _ => throw new Exception("Unsupported filter")
      }

      i = i + 1
    })

    filter.toString()
  }

}
