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

package com.couchbase.spark.query

import com.couchbase.client.scala.codec.JsonDeserializer.Passthrough
import com.couchbase.client.scala.query.{QueryScanConsistency, QueryOptions => CouchbaseQueryOptions}
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{Encoders, SparkSession}

import java.util

class QueryTableProvider extends TableProvider with Logging with DataSourceRegister {

  override def shortName(): String = "couchbase.query"

  private lazy val sparkSession = SparkSession.active
  private lazy val conf = CouchbaseConfig(sparkSession.sparkContext.getConf)

  /**
   * InferSchema is always called if the user does not pass in an explicit schema.
   *
   * @param options the options provided from the user.
   * @return the inferred schema, if possible.
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val idFieldName = Option(options.get(QueryOptions.IdFieldName)).getOrElse(DefaultConstants.DefaultIdFieldName)
    val whereClause = Option(options.get(QueryOptions.Filter)).map(p => s" WHERE $p").getOrElse("")
    val bucketName = conf.implicitBucketNameOr(options.get(QueryOptions.Bucket))
    val inferLimit = Option(options.get(QueryOptions.InferLimit)).getOrElse(DefaultConstants.DefaultInferLimit)

    val scanConsistency = Option(options.get(QueryOptions.ScanConsistency))
      .getOrElse(DefaultConstants.DefaultQueryScanConsistency)

    val opts = CouchbaseQueryOptions()
    scanConsistency match {
      case QueryOptions.NotBoundedScanConsistency => opts.scanConsistency(QueryScanConsistency.NotBounded)
      case QueryOptions.RequestPlusScanConsistency => opts.scanConsistency(QueryScanConsistency.RequestPlus())
      case v => throw new IllegalArgumentException("Unknown scanConsistency of " + v)
    }

    val scopeName = conf.implicitScopeNameOr(options.get(QueryOptions.Scope)).getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = conf.implicitCollectionName(options.get(QueryOptions.Collection)).getOrElse(DefaultConstants.DefaultCollectionName)

    val result = if (scopeName.equals(DefaultConstants.DefaultScopeName) && collectionName.equals(DefaultConstants.DefaultCollectionName)) {
      val statement = s"SELECT META().id as $idFieldName, `$bucketName`.* FROM `$bucketName`$whereClause LIMIT $inferLimit"
      logDebug(s"Inferring schema from bucket $bucketName with query '$statement'")
      CouchbaseConnection().cluster(conf).query(statement, opts)
    } else {
      val statement = s"SELECT META().id as $idFieldName, `$collectionName`.* FROM `$collectionName`$whereClause LIMIT $inferLimit"
      logDebug(s"Inferring schema from bucket/scope/collection $bucketName/$scopeName/$collectionName with query '$statement'")
      CouchbaseConnection().cluster(conf).bucket(bucketName).scope(scopeName).query(statement, opts)
    }

    val rows = result.flatMap(result => result.rowsAs[String](Passthrough.StringConvert)).get
    val ds = sparkSession.sqlContext.createDataset(rows)(Encoders.STRING)
    val schema = sparkSession.sqlContext.read.json(ds).schema

    logDebug(s"Inferred schema is $schema")

    schema
  }

  def readConfig(properties: util.Map[String, String]): QueryReadConfig = {
    QueryReadConfig(
      conf.implicitBucketNameOr(properties.get(QueryOptions.Bucket)),
      conf.implicitScopeNameOr(properties.get(QueryOptions.Scope)),
      conf.implicitCollectionName(properties.get(QueryOptions.Collection)),
      Option(properties.get(QueryOptions.IdFieldName)).getOrElse(DefaultConstants.DefaultIdFieldName),
      Option(properties.get(QueryOptions.Filter)),
      Option(properties.get(QueryOptions.ScanConsistency)).getOrElse(DefaultConstants.DefaultQueryScanConsistency)
    )
  }

  /**
   * Returns the "Table", either with an inferred schema or a user provide schema.
   *
   * @param schema the schema, either inferred or provided by the user.
   * @param partitioning partitioning information.
   * @param properties the properties for customization
   * @return the table instance which performs the actual work inside it.
   */
  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table =
    new QueryTable(schema, partitioning, properties, readConfig(properties))

  /**
   * We allow a user passing in a custom schema.
   */
  override def supportsExternalMetadata(): Boolean = true

}
