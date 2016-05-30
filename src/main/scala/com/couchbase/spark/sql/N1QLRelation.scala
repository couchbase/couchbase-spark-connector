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

import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.spark.connection.CouchbaseConfig
import com.couchbase.spark.rdd.QueryRDD
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._

/**
 * Implements a the BaseRelation for N1QL Queries.
 *
 * @param bucket the name of the bucket
 * @param userSchema the optional schema (if not provided it will be inferred)
 * @param sqlContext the sql context.
 */
class N1QLRelation(bucket: String, userSchema: Option[StructType], parameters: Map[String, String])
                  (@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan
  with Logging {

  private val cbConfig = CouchbaseConfig(sqlContext.sparkContext.getConf)
  private val bucketName = Option(bucket).getOrElse(cbConfig.buckets.head.name)
  private val idFieldName = parameters.getOrElse("idField", DefaultSource.DEFAULT_DOCUMENT_ID_FIELD)

  override val schema = userSchema.getOrElse[StructType] {
    val queryFilter = if (parameters.get("schemaFilter").isDefined) {
      "WHERE " + parameters.get("schemaFilter").get
    } else {
      ""
    }

    val query = s"SELECT META(`$bucketName`).id as `$idFieldName`, `$bucketName`.* " +
      s"FROM `$bucketName` $queryFilter LIMIT 1000"

    logInfo(s"Inferring schema from bucket $bucketName with query '$query'")

    val schema = sqlContext.read.json(
      QueryRDD(sqlContext.sparkContext, bucketName, N1qlQuery.simple(query)).map(_.value.toString)
    ).schema

    logInfo(s"Inferred schema is $schema")

    schema
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    var stringFilter = buildFilter(filters)
    if (parameters.get("schemaFilter").isDefined) {
      if (!stringFilter.isEmpty) {
        stringFilter = stringFilter + " AND "
      }
      stringFilter += parameters.get("schemaFilter").get
    }

    if (!stringFilter.isEmpty) {
      stringFilter = " WHERE " + stringFilter
    }

    val query = "SELECT " + buildColumns(requiredColumns, bucketName) + " FROM `" +
      bucketName + "`" + stringFilter

    logInfo(s"Executing generated query: '$query'")

    sqlContext.read.json(
      QueryRDD(sqlContext.sparkContext, bucketName, N1qlQuery.simple(query)).map(_.value.toString)
    ).map(row =>
      Row.fromSeq(requiredColumns.map(col => row.get(row.fieldIndex(col))).toList)
    )
  }

  /**
   * Transforms the required columns into the field list for the select statement.
   *
   * @param requiredColumns the columns to transform.
   * @return the raw N1QL string
   */
  private def buildColumns(requiredColumns: Array[String], bucktName: String): String =  {
    requiredColumns
      .map(column => {
        if (column == idFieldName) {
          s"META(`$bucketName`).id as `$idFieldName`"
        } else {
          "`" + column + "`"
        }
      })
      .mkString(",")
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

    val filter = new StringBuilder()
    var i = 0

    filters.foreach(f => {
      try {
        val encoded = N1QLRelation.filterToExpression(f)
        if (i > 0) {
          filter.append(" AND")
        }
        filter.append(encoded)
        i = i + 1
      } catch {
        case _: Exception => logInfo("Ignoring unsupported filter: " + f)
      }
    })

    filter.toString()
  }

}

object N1QLRelation {

  /**
   * Turns a filter into a N1QL expression.
   *
   * @param filter the filter to convert
   * @return the resulting expression
   */
  def filterToExpression(filter: Filter): String = {
    filter match {
      case EqualTo(attr, value) => s" ${filterAttribute(attr)} = '$value'"
      case GreaterThan(attr, value) => s" ${filterAttribute(attr)} > $value"
      case GreaterThanOrEqual(attr, value) => s" ${filterAttribute(attr)} >= $value"
      case LessThan(attr, value) => s" ${filterAttribute(attr)} < $value"
      case LessThanOrEqual(attr, value) => s" ${filterAttribute(attr)} <= $value"
      case IsNull(attr) => s" ${filterAttribute(attr)} IS NULL"
      case IsNotNull(attr) => s" ${filterAttribute(attr)} IS NOT NULL"
      case And(leftFilter, rightFilter) =>
        s" (${filterToExpression(leftFilter)} AND ${filterToExpression(rightFilter)})"
      case Or(leftFilter, rightFilter) =>
        s" (${filterToExpression(leftFilter)} OR ${filterToExpression(rightFilter)})"
      case _ => throw new Exception("Unsupported filter")
    }
  }

  private def filterAttribute(attr: String): String ={
    attr match {
      case r"(.*)${first}\.(.*)${second}" => s"`$first`.`$second`"
      case _ => s"`$attr`"
    }
  }

  implicit class Regex(sc: StringContext) {
    def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

}
