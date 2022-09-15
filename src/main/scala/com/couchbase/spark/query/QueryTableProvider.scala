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

import com.couchbase.client.core.error.DmlFailureException
import com.couchbase.client.scala.codec.JsonDeserializer.Passthrough
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.query.{QueryScanConsistency, QueryOptions => CouchbaseQueryOptions}
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection, CouchbaseConnectionPool}
import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SaveMode, SparkSession}

import scala.collection.JavaConverters._
import java.util
import scala.concurrent.duration.Duration

import com.couchbase.spark.config.optionsToSparkConf
import com.couchbase.spark.config.mapToSparkConf

class QueryTableProvider extends TableProvider with Logging with DataSourceRegister with CreatableRelationProvider {

  override def shortName(): String = "couchbase.query"

  private lazy val sparkSession = SparkSession.active

  /**
   * InferSchema is always called if the user does not pass in an explicit schema.
   *
   * @param options the options provided from the user.
   * @return the inferred schema, if possible.
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val conf = CouchbaseConfig(sparkSession.sparkContext.getConf).loadSparkOptions(options)
    if (isWrite) {
      logDebug("Not inferring schema because called from the DataFrameWriter")
      return null
    }

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
      CouchbaseConnectionPool().getConnection(conf).cluster().query(statement, opts)
    } else {
      val statement = s"SELECT META().id as $idFieldName, `$collectionName`.* FROM `$collectionName`$whereClause LIMIT $inferLimit"
      logDebug(s"Inferring schema from bucket/scope/collection $bucketName/$scopeName/$collectionName with query '$statement'")
      CouchbaseConnectionPool().getConnection(conf).cluster().bucket(bucketName).scope(scopeName).query(statement, opts)
    }

    val rows = result.flatMap(result => result.rowsAs[String](Passthrough.StringConvert)).get
    val ds = sparkSession.sqlContext.createDataset(rows)(Encoders.STRING)
    val schema = sparkSession.sqlContext.read.json(ds).schema

    logDebug(s"Inferred schema is $schema")

    schema
  }

  /**
   * This is a hack because even from the DataFrameWriter the infer schema is called - even though
   * we accept any schema.
   *
   * So check the stack where we are coming from and it allows to bail out early since we don't care
   * about the schema on a write op at all.
   *
   * @return true if we are in a write op, this is a hack.
   */
  def isWrite: Boolean =
    Thread.currentThread().getStackTrace.exists(_.getClassName.contains("DataFrameWriter"))

  /**
   * Returns the "Table", either with an inferred schema or a user provide schema.
   *
   * @param schema the schema, either inferred or provided by the user.
   * @param partitioning partitioning information.
   * @param properties the properties for customization
   * @return the table instance which performs the actual work inside it.
   */
  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table =
    new QueryTable(schema, partitioning, properties,CouchbaseConfig(sparkSession.sparkContext.getConf).loadSparkOptions(properties))

  /**
   * We allow a user passing in a custom schema.
   */
  override def supportsExternalMetadata(): Boolean = true

  override def createRelation(ctx: SQLContext, mode: SaveMode, properties: Map[String, String], data: DataFrame): BaseRelation = {
    val couchbaseConfig = CouchbaseConfig(ctx.sparkContext.getConf).loadSparkOptions(properties.asJava)
    data.toJSON.foreachPartition(new RelationPartitionWriter(couchbaseConfig, mode))

    new BaseRelation {
      override def sqlContext: SQLContext = ctx
      override def schema: StructType = data.schema
    }
  }

}

class RelationPartitionWriter(couchbaseConfig: CouchbaseConfig, mode: SaveMode)
  extends ForeachPartitionFunction[String]
    with Logging {

  override def call(t: util.Iterator[String]): Unit = {
    val scopeName = couchbaseConfig.queryConfig.scope.getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = couchbaseConfig.queryConfig.collection.getOrElse(DefaultConstants.DefaultCollectionName)

    val values = t.asScala.map(encoded => {
      val decoded = JsonObject.fromJson(encoded)
      val id = decoded.str(couchbaseConfig.queryConfig.idFieldName)
      decoded.remove(couchbaseConfig.queryConfig.idFieldName)
      s"VALUES ('$id', ${decoded.toString})"
    }).mkString(", ")

    val prefix = mode match {
      case SaveMode.ErrorIfExists | SaveMode.Ignore => "INSERT"
      case SaveMode.Overwrite => "UPSERT"
      case SaveMode.Append => throw new IllegalArgumentException("SaveMode.Append is not support with couchbase.query " +
        "DataFrame on write. Please use ErrorIfExists, Ignore or Overwrite instead.")
    }

    val statement = if (scopeName.equals(DefaultConstants.DefaultScopeName) &&
      collectionName.equals(DefaultConstants.DefaultCollectionName)) {
      s"$prefix INTO `${couchbaseConfig.queryConfig.bucket}` (KEY, VALUE) $values"
    } else {
      s"$prefix INTO `$collectionName` (KEY, VALUE) $values"
    }

    logDebug("Building and running N1QL query " + statement)

    val opts = buildOptions()
    try {
      val result = if (scopeName.equals(DefaultConstants.DefaultScopeName) && collectionName.equals(DefaultConstants.DefaultCollectionName)) {
        CouchbaseConnectionPool().getConnection(couchbaseConfig).cluster().query(statement, opts).get
      } else {
        CouchbaseConnectionPool().getConnection(couchbaseConfig).cluster().bucket(couchbaseConfig.queryConfig.bucket).scope(scopeName).query(statement, opts).get
      }

      logDebug("Completed query in: " + result.metaData.metrics.get)
    } catch {
      case e: DmlFailureException =>
        if (mode == SaveMode.Ignore) {
          logDebug("Failed to run query, but ignoring because of SaveMode.Ignore: ", e)
        } else {
          throw e
        }
    }
  }

  def buildOptions(): CouchbaseQueryOptions = {
    var opts = CouchbaseQueryOptions().metrics(true)
    couchbaseConfig.queryConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }
}
