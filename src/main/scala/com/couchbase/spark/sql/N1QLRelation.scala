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

import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.spark.Logging
import com.couchbase.spark.connection.CouchbaseConfig
import com.couchbase.spark.rdd.QueryRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.sources._

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.util.matching.Regex

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
  private val bucketName: String = if (bucket == null) {
    if (cbConfig.buckets.size != 1) {
      throw new IllegalStateException("The bucket name can only be inferred if there is "
        + "exactly 1 bucket set on the config")
    } else {
      cbConfig.buckets.head.name
    }
  } else {
    bucket
  }

  private val idFieldName = parameters.getOrElse("idField", DefaultSource.DEFAULT_DOCUMENT_ID_FIELD)
  private val timeout = parameters.get("timeout").map(v => Duration(v.toLong, MILLISECONDS))

  override val schema: StructType = userSchema.getOrElse[StructType] {
    val queryFilter = if (parameters.get("schemaFilter").isDefined) {
      "WHERE " + parameters("schemaFilter")
    } else {
      ""
    }

    val query = s"SELECT META(`$bucketName`).id as `$idFieldName`, `$bucketName`.* " +
      s"FROM `$bucketName` $queryFilter LIMIT 1000"

    logInfo(s"Inferring schema from bucket $bucketName with query '$query'")


    val rdd = QueryRDD(sqlContext.sparkContext, bucketName, N1qlQuery.simple(query), timeout)
      .map(_.value.toString)
    val dataset = sqlContext.sparkSession.createDataset(rdd)(Encoders.STRING)

    // datetime and timestamp defaults were taken from sparks Json Parser:
    // https://github.com/apache/spark/blob/branch-2.3/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/json/JSONOptions.scala#L84
    val timestampFormat = if (parameters.get("timestampFormat").isDefined) {
      parameters("timestampFormat")
    } else {
      "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    }
    val dateFormat = if (parameters.get("dateFormat").isDefined) {
      parameters("dateFormat")
    } else {
      "yyyy-MM-dd"
    }

    val schema = sqlContext
      .read
      .option("timestampFormat", timestampFormat)
      .option("dateFormat", dateFormat)
      .json(dataset)
      .schema

    logInfo(s"Inferred schema is $schema")
    schema
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    var stringFilter = buildFilter(filters)
    if (parameters.get("schemaFilter").isDefined) {
      if (!stringFilter.isEmpty) {
        stringFilter = stringFilter + " AND "
      }
      stringFilter += parameters("schemaFilter")
    }

    if (!stringFilter.isEmpty) {
      stringFilter = " WHERE " + stringFilter
    }

    val query = "SELECT " + buildColumns(requiredColumns, bucketName) + " FROM `" +
      bucketName + "`" + stringFilter

    logInfo(s"Executing generated query: '$query'")

    val rdd = QueryRDD(sqlContext.sparkContext, bucketName, N1qlQuery.simple(query), timeout)
      .map(_.value.toString)

    val dataset = sqlContext.sparkSession.createDataset(rdd)(Encoders.STRING)

    val cols = requiredColumns.map(c => new Column(c))
    sqlContext
      .read
      .schema(schema)
      .json(dataset)
      .select(cols: _*)
      .rdd
  }

  /**
   * Transforms the required columns into the field list for the select statement.
   *
   * @param requiredColumns the columns to transform.
   * @return the raw N1QL string
   */
  private def buildColumns(requiredColumns: Array[String], bucktName: String): String =  {
    if (requiredColumns.isEmpty) {
      return s"`$bucketName`.*"
    }

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
      case EqualTo(attr, value) => s" ${attrToFilter(attr)} = " + valueToFilter(value)
      case GreaterThan(attr, value) => s" ${attrToFilter(attr)} > " + valueToFilter(value)
      case GreaterThanOrEqual(attr, value) => s" ${attrToFilter(attr)} >= " + valueToFilter(value)
      case LessThan(attr, value) => s" ${attrToFilter(attr)} < " + valueToFilter(value)
      case LessThanOrEqual(attr, value) => s" ${attrToFilter(attr)} <= " + valueToFilter(value)
      case IsNull(attr) => s" ${attrToFilter(attr)} IS NULL"
      case IsNotNull(attr) => s" ${attrToFilter(attr)} IS NOT NULL"
      case StringContains(attr, value) => s" CONTAINS(${attrToFilter(attr)}, '$value')"
      case StringStartsWith(attr, value) =>
        s" ${attrToFilter(attr)} LIKE '" + escapeForLike(value) + "%'"
      case StringEndsWith(attr, value) =>
        s" ${attrToFilter(attr)} LIKE '%" + escapeForLike(value) + "'"
      case In(attr, values) =>
        val encoded = values.map(valueToFilter).mkString(",")
        s" `$attr` IN [$encoded]"
      case And(left, right) =>
        val l = filterToExpression(left)
        val r = filterToExpression(right)
        s" ($l AND $r)"
      case Or(left, right) =>
        val l = filterToExpression(left)
        val r = filterToExpression(right)
        s" ($l OR $r)"
      case Not(f) =>
        val v = filterToExpression(f)
        s" NOT ($v)"
    }
  }

  def escapeForLike(value: String): String =
    value.replaceAll("\\.", "\\\\.").replaceAll("\\*", "\\\\*")

  def valueToFilter(value: Any): String = value match {
    case v: String => s"'$v'"
    case v => s"$v"
  }

  val VerbatimRegex: Regex = """'(.*)'""".r

  def attrToFilter(attr: String): String = attr match {
    case VerbatimRegex(innerAttr) => innerAttr
    case v => v.split('.').map(elem => s"`$elem`").mkString(".")
  }

}
