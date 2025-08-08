/*
 * Copyright (c) 2025 Couchbase, Inc.
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
package com.couchbase.spark.enterpriseanalytics

import com.couchbase.analytics.client.java.{QueryOptions, QueryResult, ScanConsistency}
import com.couchbase.client.scala.analytics.{
  AnalyticsScanConsistency,
  AnalyticsOptions => CouchbaseAnalyticsOptions
}
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.columnar.ColumnarConstants.ColumnarEndpointIdx
import com.couchbase.spark.enterpriseanalytics.config.{
  EnterpriseAnalyticsConfig,
  EnterpriseAnalyticsConnection
}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{Encoders, SparkSession}

import java.util
import scala.jdk.CollectionConverters._

class EnterpriseAnalyticsTableProvider extends TableProvider with Logging with DataSourceRegister {

  override def shortName(): String = "couchbase.enterprise-analytics"

  private lazy val sparkSession = SparkSession.active

  private def requireOption(options: CaseInsensitiveStringMap, key: String): String = {
    if (!options.containsKey(key)) {
      throw new IllegalArgumentException(s"Option '${key}' must be provided")
    }
    options.get(key)
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val databaseName   = requireOption(options, EnterpriseAnalyticsOptions.Database)
    val scopeName      = requireOption(options, EnterpriseAnalyticsOptions.Scope)
    val collectionName = requireOption(options, EnterpriseAnalyticsOptions.Collection)
    val keyspace       = s"`${databaseName}`.`${scopeName}`.`${collectionName}`"

    val conf = EnterpriseAnalyticsConfig(sparkSession.sparkContext.getConf)

    val whereClause =
      Option(options.get(EnterpriseAnalyticsOptions.Filter)).map(p => s" WHERE $p").getOrElse("")
    val inferLimit =
      Option(options.get(EnterpriseAnalyticsOptions.InferLimit))
        .getOrElse(DefaultConstants.DefaultInferLimit)

    val scanConsistency = Option(options.get(EnterpriseAnalyticsOptions.ScanConsistency))
      .getOrElse(DefaultConstants.DefaultAnalyticsScanConsistency)

    var opts = CouchbaseAnalyticsOptions().endpointIdx(ColumnarEndpointIdx)
    scanConsistency match {
      case EnterpriseAnalyticsOptions.NotBoundedScanConsistency =>
        opts = opts.scanConsistency(AnalyticsScanConsistency.NotBounded)
      case EnterpriseAnalyticsOptions.RequestPlusScanConsistency =>
        opts = opts.scanConsistency(AnalyticsScanConsistency.RequestPlus)
      case v => throw new IllegalArgumentException("Unknown scanConsistency of " + v)
    }

    val statement =
      s"SELECT c.* FROM ${keyspace} as c $whereClause LIMIT $inferLimit"
    logDebug(
      s"Inferring schema with query '$statement'"
    )
    val result: QueryResult = EnterpriseAnalyticsConnection()
      .cluster(conf)
      .executeQuery(
        statement,
        (queryOpts: QueryOptions) => {
          scanConsistency match {
            case EnterpriseAnalyticsOptions.NotBoundedScanConsistency =>
              queryOpts.scanConsistency(ScanConsistency.NOT_BOUNDED)
            case EnterpriseAnalyticsOptions.RequestPlusScanConsistency =>
              queryOpts.scanConsistency(ScanConsistency.REQUEST_PLUS)
            case v => throw new IllegalArgumentException("Unknown scanConsistency of " + v)
          }
        }
      )

    val rows   = result.rows().asScala.map(_.toString)
    val ds     = sparkSession.sqlContext.createDataset(rows.toSeq)(Encoders.STRING)
    val schema = sparkSession.sqlContext.read.json(ds).schema

    logDebug(s"Inferred schema is $schema")

    schema
  }

  def readConfig(properties: util.Map[String, String]): EnterpriseAnalyticsReadConfig = {
    val databaseName = Option(properties.get(EnterpriseAnalyticsOptions.Database))
      .orElse(throw new IllegalArgumentException("Database must be provided"))
      .get
    val scopeName = Option(properties.get(EnterpriseAnalyticsOptions.Scope))
      .orElse(throw new IllegalArgumentException("Scope must be provided"))
      .get
    val collectionName = Option(properties.get(EnterpriseAnalyticsOptions.Collection))
      .orElse(throw new IllegalArgumentException("Collection must be provided"))
      .get

    EnterpriseAnalyticsReadConfig(
      databaseName,
      scopeName,
      collectionName,
      Option(properties.get(EnterpriseAnalyticsOptions.Filter)),
      Option(properties.get(EnterpriseAnalyticsOptions.ScanConsistency))
        .getOrElse(DefaultConstants.DefaultAnalyticsScanConsistency),
      Option(properties.get(EnterpriseAnalyticsOptions.Timeout))
    )
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table =
    new EnterpriseAnalyticsTable(schema, readConfig(properties))

  /** We allow a user passing in a custom schema.
    */
  override def supportsExternalMetadata(): Boolean = true

}
