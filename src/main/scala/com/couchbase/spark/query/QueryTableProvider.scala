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

import com.couchbase.client.core.error.{DmlFailureException, InvalidArgumentException, DocumentExistsException}
import com.couchbase.client.scala.codec.JsonDeserializer.Passthrough
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.query.{QueryScanConsistency, QueryOptions => CouchbaseQueryOptions}
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.query.QueryOptions.{PartitionColumn, PartitionCount, PartitionLowerBound, PartitionUpperBound}
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
import java.util.UUID
import scala.concurrent.duration.{Duration, NANOSECONDS}

class QueryTableProvider
    extends TableProvider
    with Logging
    with DataSourceRegister
    with CreatableRelationProvider {

  override def shortName(): String = "couchbase.query"

  private lazy val sparkSession = SparkSession.active

  /** InferSchema is always called if the user does not pass in an explicit schema.
    *
    * @param options
    *   the options provided from the user.
    * @return
    *   the inferred schema, if possible.
    */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (isWrite) {
      logDebug("Not inferring schema because called from the DataFrameWriter")
      return null
    }

    val connectionIdentifier = Option(options.get(QueryOptions.ConnectionIdentifier))

    val conf = CouchbaseConfig(
      sparkSession.sparkContext.getConf,
      connectionIdentifier
    )

    val idFieldName =
      Option(options.get(QueryOptions.IdFieldName)).getOrElse(DefaultConstants.DefaultIdFieldName)
    val whereClause = Option(options.get(QueryOptions.Filter)).map(p => s" WHERE $p").getOrElse("")
    val bucketName  = conf.implicitBucketNameOr(options.get(QueryOptions.Bucket))
    val inferLimit =
      Option(options.get(QueryOptions.InferLimit)).getOrElse(DefaultConstants.DefaultInferLimit)

    val scanConsistency = Option(options.get(QueryOptions.ScanConsistency))
      .getOrElse(DefaultConstants.DefaultQueryScanConsistency)

    val newOpts = CouchbaseQueryOptions()
    val opts = scanConsistency match {
      case QueryOptions.NotBoundedScanConsistency =>
        newOpts.scanConsistency(QueryScanConsistency.NotBounded)
      case QueryOptions.RequestPlusScanConsistency =>
        newOpts.scanConsistency(QueryScanConsistency.RequestPlus())
      case v => throw new IllegalArgumentException("Unknown scanConsistency of " + v)
    }

    val scopeName = conf
      .implicitScopeNameOr(options.get(QueryOptions.Scope))
      .getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = conf
      .implicitCollectionName(options.get(QueryOptions.Collection))
      .getOrElse(DefaultConstants.DefaultCollectionName)

    val result =
      if (
        scopeName.equals(DefaultConstants.DefaultScopeName) && collectionName.equals(
          DefaultConstants.DefaultCollectionName
        )
      ) {
        val statement =
          s"SELECT META().id as $idFieldName, `$bucketName`.* FROM `$bucketName`$whereClause LIMIT $inferLimit"
        logDebug(s"Inferring schema from bucket $bucketName with query '$statement'")
        CouchbaseConnection(connectionIdentifier).cluster(conf).query(statement, opts)
      } else {
        val statement =
          s"SELECT META().id as $idFieldName, `$collectionName`.* FROM `$collectionName`$whereClause LIMIT $inferLimit"
        logDebug(
          s"Inferring schema from bucket/scope/collection $bucketName/$scopeName/$collectionName with query '$statement'"
        )
        CouchbaseConnection(connectionIdentifier)
          .cluster(conf)
          .bucket(bucketName)
          .scope(scopeName)
          .query(statement, opts)
      }

    val rows   = result.flatMap(result => result.rowsAs[String](Passthrough.StringConvert)).get.toSeq
    val ds     = sparkSession.sqlContext.createDataset(rows)(Encoders.STRING)
    val schema = sparkSession.sqlContext.read.json(ds).schema

    logDebug(s"Inferred schema is $schema")

    schema
  }

  /** This is a hack because even from the DataFrameWriter the infer schema is called - even though
    * we accept any schema.
    *
    * So check the stack where we are coming from and it allows to bail out early since we don't
    * care about the schema on a write op at all.
    *
    * @return
    *   true if we are in a write op, this is a hack.
    */
  def isWrite: Boolean =
    Thread.currentThread().getStackTrace.exists(_.getClassName.contains("DataFrameWriter"))

  def readConfig(properties: util.Map[String, String]): QueryReadConfig = {
    val connectionIdentifier = Option(properties.get(QueryOptions.ConnectionIdentifier))

    val conf = CouchbaseConfig(
      sparkSession.sparkContext.getConf,
      connectionIdentifier
    )

    val partitionColumn = Option(properties.get(PartitionColumn))
    val lowerBound = Option(properties.get(PartitionLowerBound))
    val upperBound = Option(properties.get(PartitionUpperBound))
    val numPartitions = Option(properties.get(PartitionCount))

    val partitionOptionCount = partitionColumn.count(_ => true) + lowerBound.count(_ => true) + upperBound.count(_ => true) + numPartitions.count(_ => true)
    if (partitionOptionCount > 0 && partitionOptionCount < 4) {
      throw new InvalidArgumentException("If one of partitionColumn, partitionLowerBound, partitionUpperBound and partitionCount arguments are provided, then all must be", null, null)
    }

    val partitioning = if (partitionColumn.isDefined && lowerBound.isDefined && upperBound.isDefined && numPartitions.isDefined) {
      val lb = lowerBound.get.toLong
      val ub = upperBound.get.toLong
      val np = numPartitions.get.toLong
      if (lb >= ub) {
        throw new InvalidArgumentException(s"${PartitionLowerBound} must be < ${PartitionUpperBound}", null, null)
      }
      if (np <= 0) {
        throw new InvalidArgumentException(s"${PartitionCount} must be > 0", null, null)
      }
      Some(PartitioningConfig(partitionColumn.get, lb, ub, np))
    } else None

    QueryReadConfig(
      conf.implicitBucketNameOr(properties.get(QueryOptions.Bucket)),
      conf.implicitScopeNameOr(properties.get(QueryOptions.Scope)),
      conf.implicitCollectionName(properties.get(QueryOptions.Collection)),
      Option(properties.get(QueryOptions.IdFieldName))
        .getOrElse(DefaultConstants.DefaultIdFieldName),
      Option(properties.get(QueryOptions.Filter)),
      Option(properties.get(QueryOptions.ScanConsistency))
        .getOrElse(DefaultConstants.DefaultQueryScanConsistency),
      Option(properties.get(QueryOptions.Timeout)),
      Option(properties.get(QueryOptions.PushDownAggregate)).getOrElse("true").toBoolean,
      connectionIdentifier,
      partitioning
    )
  }

  def writeConfig(properties: util.Map[String, String], conf: CouchbaseConfig): QueryWriteConfig = {
    val connectionIdentifier = Option(properties.get(QueryOptions.ConnectionIdentifier))

    QueryWriteConfig(
      conf.implicitBucketNameOr(properties.get(QueryOptions.Bucket)),
      conf.implicitScopeNameOr(properties.get(QueryOptions.Scope)),
      conf.implicitCollectionName(properties.get(QueryOptions.Collection)),
      Option(properties.get(QueryOptions.IdFieldName))
        .getOrElse(DefaultConstants.DefaultIdFieldName),
      Option(properties.get(QueryOptions.Timeout)),
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
    new QueryTable(schema, partitioning, properties, readConfig(properties))

  /** We allow a user passing in a custom schema.
    */
  override def supportsExternalMetadata(): Boolean = true

  override def createRelation(
      ctx: SQLContext,
      mode: SaveMode,
      properties: Map[String, String],
      data: DataFrame
  ): BaseRelation = {
    val couchbaseConfig = CouchbaseConfig(
      ctx.sparkContext.getConf,
      properties.get(QueryOptions.ConnectionIdentifier)
    )
    val writeConfig = this.writeConfig(properties.asJava, couchbaseConfig)
    data.toJSON.foreachPartition(new RelationPartitionWriter(writeConfig, couchbaseConfig, mode))

    new BaseRelation {
      override def sqlContext: SQLContext = ctx
      override def schema: StructType     = data.schema
    }
  }

}

object RelationPartitionWriter {
  val MaxStatementLengthChars = 2000
}

class RelationPartitionWriter(
    writeConfig: QueryWriteConfig,
    couchbaseConfig: CouchbaseConfig,
    mode: SaveMode
) extends ForeachPartitionFunction[String]
    with Logging {

  def truncateStatement(statement: String, maxLength: Int): String = {
    if (statement.length <= maxLength) {
      statement
    } else {
      val partLength = (maxLength - 3) / 2
      statement.take(partLength) + "..." + statement.takeRight(partLength)
    }
  }

  override def call(t: util.Iterator[String]): Unit = {
    val queryUuid      = UUID.randomUUID.toString.substring(0, 6)
    val scopeName      = writeConfig.scope.getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = writeConfig.collection.getOrElse(DefaultConstants.DefaultCollectionName)
    val started        = System.nanoTime()
    var count          = 0

    val values = t.asScala
      .map(encoded => {
        // Going non-functional to avoid iterating twice or buffering full `t` (which is of unknown size)
        count += 1
        val decoded = JsonObject.fromJson(encoded)
        val id      = decoded.str(writeConfig.idFieldName)
        decoded.remove(writeConfig.idFieldName)
        s"VALUES ('$id', ${decoded.toString})"
      })
      .mkString(", ")

    val prefix = mode match {
      case SaveMode.ErrorIfExists | SaveMode.Ignore => "INSERT"
      case SaveMode.Overwrite                       => "UPSERT"
      case SaveMode.Append =>
        throw new IllegalArgumentException(
          "SaveMode.Append is not support with couchbase.query " +
            "DataFrame on write. Please use ErrorIfExists, Ignore or Overwrite instead."
        )
    }

    val statement =
      if (
        scopeName.equals(DefaultConstants.DefaultScopeName) &&
        collectionName.equals(DefaultConstants.DefaultCollectionName)
      ) {
        s"$prefix INTO `${writeConfig.bucket}` (KEY, VALUE) $values"
      } else {
        s"$prefix INTO `$collectionName` (KEY, VALUE) $values"
      }

    // These write statements can be very large
    if (statement.length > RelationPartitionWriter.MaxStatementLengthChars) {
      logInfo(s"Building and running write SQL++ query ${queryUuid} for ${count} values (truncated statement - full statement available at trace level) "
        + truncateStatement(statement, RelationPartitionWriter.MaxStatementLengthChars))
      logTrace(s"Building and running write SQL++ query ${queryUuid} for ${count} values (full statement) " + statement)
    } else {
      logInfo(s"Building and running write SQL++ query ${queryUuid} for ${count} values (full statement) " + statement)
    }

    val opts = buildOptions()
    try {
      val result =
        if (
          scopeName.equals(DefaultConstants.DefaultScopeName) && collectionName.equals(
            DefaultConstants.DefaultCollectionName
          )
        ) {
          CouchbaseConnection(writeConfig.connectionIdentifier)
            .cluster(couchbaseConfig)
            .query(statement, opts)
            .get
        } else {
          CouchbaseConnection(writeConfig.connectionIdentifier)
            .cluster(couchbaseConfig)
            .bucket(writeConfig.bucket)
            .scope(scopeName)
            .query(statement, opts)
            .get
        }

      val m = result.metaData.metrics.get
      logInfo(s"Completed write query ${queryUuid} in ${NANOSECONDS.toMillis(System.nanoTime() - started)}ms.  Metrics from query service: elapsedTime=${m.elapsedTime.toMillis}ms executionTime=${m.executionTime.toMillis}ms mutationCount=${m.mutationCount} errorCount=${m.errorCount} warningCount=${m.warningCount}")
    } catch {
      case e @ (_: DmlFailureException | _: DocumentExistsException) =>
        if (mode == SaveMode.Ignore) {
          logInfo("Failed to run query, but ignoring because of SaveMode.Ignore: ", e)
        } else {
          throw e
        }
    }
  }

  def buildOptions(): CouchbaseQueryOptions = {
    var opts = CouchbaseQueryOptions().metrics(true)
    writeConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }
}
