/*
 * Copyright (c) 2021 Couchbase, Inc.
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

import com.couchbase.spark.core.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.rdd.QueryRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, Encoders, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

import scala.util.matching.Regex
import org.apache.spark.sql.sources._

class QueryRelation(
  private[spark] val connection: Broadcast[CouchbaseConnection],
  private[spark] val options: CouchbaseSqlQueryOptions,
)(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with PrunedFilteredScan
    with Logging {

  private val globalConfig = CouchbaseConfig(sqlContext.sparkContext.getConf)
  private val bucketName = connection.value.bucketName(globalConfig, options.bucketName)

  override def schema: StructType = options.explicitSchema.getOrElse[StructType] {
    val whereClause = options.schemaFilter match {
      case Some(clause) => "WHERE " + clause
      case None => ""
    }

    val statement = s"SELECT META().id as ${options.idFieldName}, `$bucketName`.* " +
      s"FROM `$bucketName` $whereClause LIMIT 1000"

    logDebug(s"Inferring schema from bucket $bucketName with query '$statement'")

    val rdd = new QueryRDD(sqlContext.sparkContext, statement)
      .flatMap(result => result.rows)
      .map(row => row.toString())
    val dataset = sqlContext.sparkSession.createDataset(rdd)(Encoders.STRING)

    val schema = sqlContext
      .read
      .options(options.schemaOptions)
      .json(dataset)
      .schema

    logDebug(s"Inferred schema is $schema")
    schema

  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    var whereClause = compileFilter(filters)
    options.schemaFilter match {
      case Some(clause) => {
        if (whereClause.nonEmpty) {
          whereClause = whereClause + " AND "
        }
        whereClause += clause
      }
      case _ =>
    }

    if (whereClause.nonEmpty) {
      whereClause = " WHERE " + whereClause
    }

    val statement = "SELECT " + buildColumns(requiredColumns, bucketName) + " FROM `" +
      bucketName + "`" + whereClause

    logDebug(s"Executing generated query: '$statement'")

    val rdd = new QueryRDD(sqlContext.sparkContext, statement)
      .flatMap(result => result.rows)
      .map(row => row.toString())
    val dataset = sqlContext.sparkSession.createDataset(rdd)(Encoders.STRING)

    val columns = requiredColumns.map(c => new Column(c))
    sqlContext
      .read
      .schema(schema)
      .json(dataset)
      .select(columns: _*)
      .rdd
  }

  /** Transforms the required columns into the field list for the select statement.
   *
   * @param requiredColumns the columns to transform.
   * @return the raw N1QL string
   */
  private def buildColumns(requiredColumns: Array[String], bucketName: String): String =  {
    if (requiredColumns.isEmpty) {
      return s"`$bucketName`.*"
    }

    requiredColumns
      .map(column => {
        if (column == options.idFieldName) {
          s"META(`$bucketName`).id as `${options.idFieldName}`"
        } else {
          "`" + column + "`"
        }
      })
      .mkString(",")
  }

  /** Transform the filters into a N1QL where clause.
   *
   * @todo In, And, Or, Not filters including recursion
   * @param filters the filters to transform
   * @return the transformed raw N1QL clause
   */
  def  compileFilter(filters: Array[Filter]): String = {
    if (filters.isEmpty) {
      return ""
    }

    val filter = new StringBuilder()
    var i = 0

    filters.foreach(f => {
      try {
        val encoded = filterToExpression(f)
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

  /**
   * Turns a filter into a N1QL expression.
   *
   * @param filter the filter to convert
   * @return the resulting expression
   */
  def filterToExpression(filter: Filter): String = {
    filter match {
      case AlwaysFalse() => " FALSE"
      case AlwaysTrue() => " TRUE"
      case And(left, right) =>
        val l = filterToExpression(left)
        val r = filterToExpression(right)
        s" ($l AND $r)"
      case EqualNullSafe(attr, value) => s" (NOT (${attrToFilter(attr)} != "+valueToFilter(value)+s" OR ${attrToFilter(attr)} IS NULL OR "+valueToFilter(value)+s" IS NULL) OR (${attrToFilter(attr)} IS NULL AND " + valueToFilter(value) + " IS NULL))"
      case EqualTo(attr, value) => s" ${attrToFilter(attr)} = " + valueToFilter(value)
      case GreaterThan(attr, value) => s" ${attrToFilter(attr)} > " + valueToFilter(value)
      case GreaterThanOrEqual(attr, value) => s" ${attrToFilter(attr)} >= " + valueToFilter(value)
      case In(attr, values) =>
        val encoded = values.map(valueToFilter).mkString(",")
        s" `$attr` IN [$encoded]"
      case IsNotNull(attr) => s" ${attrToFilter(attr)} IS NOT NULL"
      case IsNull(attr) => s" ${attrToFilter(attr)} IS NULL"
      case LessThan(attr, value) => s" ${attrToFilter(attr)} < " + valueToFilter(value)
      case LessThanOrEqual(attr, value) => s" ${attrToFilter(attr)} <= " + valueToFilter(value)
      case Not(f) =>
        val v = filterToExpression(f)
        s" NOT ($v)"
      case Or(left, right) =>
        val l = filterToExpression(left)
        val r = filterToExpression(right)
        s" ($l OR $r)"
      case StringContains(attr, value) => s" CONTAINS(${attrToFilter(attr)}, '$value')"
      case StringEndsWith(attr, value) =>
        s" ${attrToFilter(attr)} LIKE '%" + escapeForLike(value) + "'"
      case StringStartsWith(attr, value) =>
        s" ${attrToFilter(attr)} LIKE '" + escapeForLike(value) + "%'"
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

/** Allows to customize the way the Query relation is created and queried.
 *
 * @param explicitSchema use this to manually provide the schema, instead of using schema inference.
 * @param schemaFilter allows to provide a custom WHERE clause (without the WHERE) for schema inference.
 * @param schemaOptions allows to provide a custom map that is passed down to Spark JSON schema inference.
 * @param bucketName allows to explicitly provide a different bucket name.
 * @param idFieldName the field name the connector will use to identify the document ID.
 */
case class CouchbaseSqlQueryOptions(

 /** Use this to manually provide the schema, instead of using schema inference. */
  explicitSchema: Option[StructType] = None,

  /** Allows to provide a custom WHERE clause (without the WHERE) for schema inference.
   *
   * This option can be used to provide parts of a WHERE clause to further narrow down the
   * query that should be used for schema inference. As an example, if you want to filter
   * by a type field in your document, the string could look like "type = 'airport'" or
   * similar. At debug level, the query and the inferred schema are logged for easier
   * debugging.
   */
  schemaFilter: Option[String] = None,

  /** Allows to provide a custom map that is passed down to Spark JSON schema inference.
   *
   * Using this map, for example custom timestampFormat or dateFormat can be provided. See
   * the documentation for the spark JSON source for more details on what is possible.
   */
  schemaOptions: Map[String, String] = Map[String, String](),

  /** Allows to explicitly provide a different bucket name.
   *
   * If none provided, the implicit one will be used.
   */
  bucketName: Option[String] = None,

  /** The field name the connector will use to identify the document ID.
   *
   * If not provided, a sane default will be picked that is unlikely to collide with
   * your document model, but just in case it can be configured here.
   */
  idFieldName: String
)