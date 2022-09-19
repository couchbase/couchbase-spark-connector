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
package com.couchbase.spark.kv

import com.couchbase.client.core.error.DocumentExistsException
import com.couchbase.client.scala.codec.RawJsonTranscoder
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.{InsertOptions, UpsertOptions}
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.config._
import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.types.{BinaryType, BooleanType, IntegerType, LongType, MapType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.collection.JavaConverters._
import java.util
import scala.concurrent.duration.Duration

class KeyValueTableProvider
  extends Logging
  with TableProvider
  with DataSourceRegister
  with CreatableRelationProvider {

  private lazy val sparkSession = SparkSession.active

  override def shortName(): String = "couchbase.kv"

  override def createRelation(ctx: SQLContext, mode: SaveMode, parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val couchbaseConfig = CouchbaseConfig(ctx.sparkContext.getConf,false).loadDSOptions(parameters.asJava)
    data.toJSON.foreachPartition(new RelationPartitionWriter(couchbaseConfig, mode))

    if (mode == SaveMode.Append) {
      throw new IllegalArgumentException("SaveMode.Append is not supported - please use the other SaveMode types.")
    }

    new BaseRelation {
      override def sqlContext: SQLContext = ctx
      override def schema: StructType = data.schema
    }
  }

  def streamConfig(properties: util.Map[String, String], couchbaseConfig: CouchbaseConfig): KeyValueStreamConfig = {
    val defaultNumPartitions = SparkSession.active.sparkContext.defaultParallelism

    var collections: Seq[String] = Seq()

    val userCollection = couchbaseConfig.implicitCollectionName(properties.get(DSConfigOptions.Collection))
    if (userCollection.isDefined) {
      collections = collections :+ userCollection.get
    }
    val userCollections = couchbaseConfig.implicitCollectionName(properties.get(DSConfigOptions.Collections))
    if (userCollections.isDefined) {
      collections = collections ++ userCollections.get.split(",")
    }

    val meta = properties.get(DSConfigOptions.StreamMetaData)
    val streamXattrs = meta != null && meta.equals(DSConfigOptions.StreamMetaDataFull)

    val streamFrom = properties.get(DSConfigOptions.StreamFrom) match {
      case DSConfigOptions.StreamFromNow => StreamFromVariants.FromNow
      case DSConfigOptions.StreamFromBeginning => StreamFromVariants.FromBeginning
      case null => throw new IllegalArgumentException("A KeyValueOptions.StreamFrom option must be provided")
      case v => throw new IllegalArgumentException("Unknown KeyValueOptions.StreamFrom option: " + v)
    }

    val c = KeyValueStreamConfig(
      streamFrom,
      Option(properties.get(DSConfigOptions.NumPartitions)).getOrElse(defaultNumPartitions.toString).toInt,
      collections,
      Option(properties.get(DSConfigOptions.StreamContent)).getOrElse("true").toBoolean,
      streamXattrs,
      Option(properties.get(DSConfigOptions.StreamFlowControlBufferSize)).map(_.toInt),
      Option(properties.get(DSConfigOptions.StreamPersistencePollingInterval)),
      couchbaseConfig
    )
    logDebug(s"Using KeyValueStreamConfig of $c")
    c
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val always = if (options.containsKey(DSConfigOptions.StreamContent) && !options.get(DSConfigOptions.StreamContent).toBoolean) {
      Seq(
        StructField("id", StringType, nullable = false),
        StructField("deletion", BooleanType, nullable = false)
      )
    } else {
      Seq(
        StructField("id", StringType, nullable = false),
        StructField("content", BinaryType, nullable = true),
        StructField("deletion", BooleanType, nullable = false)
      )
    }

    val basic = Seq(
      StructField("cas", LongType, nullable = false),
      StructField("scope", StringType, nullable = true),
      StructField("collection", StringType, nullable = true),
    )
    val full = Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("vbucket", IntegerType, nullable = false),
      StructField("xattrs", MapType(StringType, StringType), nullable = true)
    )

    val fields = options.get(DSConfigOptions.StreamMetaData) match {
      case DSConfigOptions.StreamMetaDataNone => always
      case null | DSConfigOptions.StreamMetaDataBasic => always ++ basic
      case DSConfigOptions.StreamMetaDataFull => always ++ basic ++ full
    }

    logDebug(s"Streaming the following fields $fields")

    StructType(fields)
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    new KeyValueTable(schema, streamConfig(properties,CouchbaseConfig(sparkSession.sparkContext.getConf,false).loadDSOptions(properties)))
  }

}

class RelationPartitionWriter(couchbaseConfig: CouchbaseConfig, mode: SaveMode)
  extends ForeachPartitionFunction[String]
    with Logging {
  override def call(t: util.Iterator[String]): Unit = {
    val scopeName = couchbaseConfig.dsConfig.scope.getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = couchbaseConfig.dsConfig.collection.getOrElse(DefaultConstants.DefaultCollectionName)

    val keyValues = t.asScala.map(encoded => {
      val decoded = JsonObject.fromJson(encoded)
      val id = decoded.str(couchbaseConfig.dsConfig.idFieldName)
      decoded.remove(couchbaseConfig.dsConfig.idFieldName)
      (id, decoded.toString)
    }).toList

    val coll = CouchbaseConnectionPool()
      .getConnection(couchbaseConfig)
      .cluster()
      .bucket(couchbaseConfig.dsConfig.bucket)
      .scope(scopeName)
      .collection(collectionName)

    val durability = couchbaseConfig.dsConfig.durability match {
      case Some(v) if v == DSConfigOptions.MajorityDurability => Durability.Majority
      case Some(v) if v == DSConfigOptions.MajorityAndPersistToActiveDurability => Durability.MajorityAndPersistToActive
      case Some(v) if v == DSConfigOptions.PersistToMajorityDurability => Durability.PersistToMajority
      case None => Durability.Disabled
      case d => throw new IllegalArgumentException("Unknown/Unsupported durability provided: " + d)
    }

    SFlux
      .fromIterable(keyValues)
      .flatMap(v => {
        mode match {
          case SaveMode.Overwrite =>
            coll.reactive.upsert(v._1, v._2, buildUpsertOptions(durability))
          case SaveMode.ErrorIfExists =>
            coll.reactive.insert(v._1, v._2, buildInsertOptions(durability))
          case SaveMode.Ignore =>
            coll.reactive.insert(v._1, v._2, buildInsertOptions(durability)).onErrorResume(t => {
              if (t.isInstanceOf[DocumentExistsException]) {
                SMono.empty
              } else {
                SMono.error(t)
              }
            })
          case m =>
            throw new IllegalStateException("Unsupported SaveMode: " + m)
        }
      })
      .blockLast()
  }

  def buildInsertOptions(durability: Durability): InsertOptions = {
    var opts = InsertOptions().transcoder(RawJsonTranscoder.Instance).durability(durability)
    couchbaseConfig.dsConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }

  def buildUpsertOptions(durability: Durability): UpsertOptions = {
    var opts = UpsertOptions().transcoder(RawJsonTranscoder.Instance).durability(durability)
    couchbaseConfig.dsConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }

}