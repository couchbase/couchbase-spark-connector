/*
 * Copyright (c) 2024 Couchbase, Inc.
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
package com.couchbase.spark.columnar

import com.couchbase.client.scala.analytics.{AnalyticsScanConsistency, AnalyticsOptions => CouchbaseAnalyticsOptions}
import com.couchbase.client.scala.codec.JsonDeserializer.Passthrough
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.columnar.ColumnarConstants.ColumnarEndpointIdx
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.query.QueryOptions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{Encoders, SparkSession}

import java.util

class ColumnarTableProvider extends TableProvider with Logging with DataSourceRegister {

  override def shortName(): String = "couchbase.columnar"

  private lazy val sparkSession = SparkSession.active

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val connectionIdentifier = Option(options.get(QueryOptions.ConnectionIdentifier))

    val conf = CouchbaseConfig(
      sparkSession.sparkContext.getConf,
      connectionIdentifier
    )

    val whereClause =
      Option(options.get(ColumnarOptions.Filter)).map(p => s" WHERE $p").getOrElse("")
    val collection = options.get(ColumnarOptions.Collection)
    val inferLimit =
      Option(options.get(ColumnarOptions.InferLimit)).getOrElse(DefaultConstants.DefaultInferLimit)

    val scanConsistency = Option(options.get(ColumnarOptions.ScanConsistency))
      .getOrElse(DefaultConstants.DefaultAnalyticsScanConsistency)

    var opts = CouchbaseAnalyticsOptions().endpointIdx(ColumnarEndpointIdx)
    scanConsistency match {
      case ColumnarOptions.NotBoundedScanConsistency =>
        opts = opts.scanConsistency(AnalyticsScanConsistency.NotBounded)
      case ColumnarOptions.RequestPlusScanConsistency =>
        opts = opts.scanConsistency(AnalyticsScanConsistency.RequestPlus)
      case v => throw new IllegalArgumentException("Unknown scanConsistency of " + v)
    }

    // Not using the implicit bucket and scope names as that will cause more confusion than it solves.
    val databaseName = Option(options.get(ColumnarOptions.Database)).orElse(throw new IllegalArgumentException("Database must be provided")).get
    val scopeName  = Option(options.get(ColumnarOptions.Scope)).orElse(throw new IllegalArgumentException("Scope must be provided")).get

    opts = opts.raw(Map("query_context" -> ColumnarQueryContext.queryContext(databaseName, scopeName)))

    val statement =
      s"SELECT `$collection`.* FROM `$collection`$whereClause LIMIT $inferLimit"
    logDebug(
      s"Inferring schema from bucket/scope/dataset $databaseName/$scopeName/$collection with query '$statement'"
    )
    val result = CouchbaseConnection(connectionIdentifier)
      .cluster(conf)
      .analyticsQuery(statement, opts)

    val rows   = result.flatMap(result => result.rowsAs[String](Passthrough.StringConvert)).get
    val ds     = sparkSession.sqlContext.createDataset(rows.toSeq)(Encoders.STRING)
    val schema = sparkSession.sqlContext.read.json(ds).schema

    logDebug(s"Inferred schema is $schema")

    schema
  }

  def readConfig(properties: util.Map[String, String]): ColumnarReadConfig = {
    val connectionIdentifier = Option(properties.get(ColumnarOptions.ConnectionIdentifier))

    val collection = properties.get(ColumnarOptions.Collection)
    if (collection == null) {
      throw new IllegalArgumentException("A dataset must be provided through the options!")
    }

    val databaseName = Option(properties.get(ColumnarOptions.Database)).orElse(throw new IllegalArgumentException("Database must be provided")).get
    val scopeName  = Option(properties.get(ColumnarOptions.Scope)).orElse(throw new IllegalArgumentException("Scope must be provided")).get

    ColumnarReadConfig(
      collection,
      databaseName,
      scopeName,
      Option(properties.get(ColumnarOptions.Filter)),
      Option(properties.get(ColumnarOptions.ScanConsistency))
        .getOrElse(DefaultConstants.DefaultAnalyticsScanConsistency),
      Option(properties.get(ColumnarOptions.Timeout)),
      connectionIdentifier
    )
  }

  def writeConfig(properties: util.Map[String, String]): ColumnarWriteConfig = {
    val connectionIdentifier = Option(properties.get(ColumnarOptions.ConnectionIdentifier))

    val collection = properties.get(ColumnarOptions.Collection)
    if (collection == null) {
      throw new IllegalArgumentException("A dataset must be provided through the options!")
    }

    val databaseName = Option(properties.get(ColumnarOptions.Database)).orElse(throw new IllegalArgumentException("Database must be provided")).get
    val scopeName  = Option(properties.get(ColumnarOptions.Scope)).orElse(throw new IllegalArgumentException("Scope must be provided")).get

    ColumnarWriteConfig(
      collection,
      databaseName,
      scopeName,
      Option(properties.get(ColumnarOptions.Filter)),
      Option(properties.get(ColumnarOptions.Timeout)),
      connectionIdentifier
    )
  }

  /** Returns the "Table", either with an inferred schema or a user provide schema.
    *
    * @param schema
    *   the schema, either inferred or provided by the user.
    * @param partitioning
    *   partitioning information.
    * @param properties
    *   the properties for customization
    * @return
    *   the table instance which performs the actual work inside it.
    */
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table =
    new ColumnarTable(schema, partitioning, properties, readConfig(properties), writeConfig(properties))

  /** We allow a user passing in a custom schema.
    */
  override def supportsExternalMetadata(): Boolean = true

}
