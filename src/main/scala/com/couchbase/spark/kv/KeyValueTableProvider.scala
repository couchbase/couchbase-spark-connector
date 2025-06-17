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

import com.couchbase.client.core.error.{CouchbaseException, DocumentExistsException}
import com.couchbase.client.scala.codec.RawJsonTranscoder
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.{InsertOptions, ReplaceOptions, UpsertOptions}
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
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
import scala.util.{Failure, Success}

class KeyValueTableProvider
    extends Logging
    with TableProvider
    with DataSourceRegister
    with CreatableRelationProvider {

  override def shortName(): String = "couchbase.kv"

  override def createRelation(
      ctx: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame
  ): BaseRelation = {
    val couchbaseConfig = CouchbaseConfig(
      ctx.sparkContext.getConf,
      parameters.get(KeyValueOptions.ConnectionIdentifier)
    )
    data.toJSON.foreachPartition(
      new RelationPartitionWriter(
        writeConfig(parameters.asJava, couchbaseConfig),
        couchbaseConfig,
        mode
      )
    )

    if (mode == SaveMode.Append) {
      throw new IllegalArgumentException(
        "SaveMode.Append is not supported - please use the other SaveMode types."
      )
    }

    new BaseRelation {
      override def sqlContext: SQLContext = ctx
      override def schema: StructType     = data.schema
    }
  }

  def writeConfig(
      properties: util.Map[String, String],
      conf: CouchbaseConfig
  ): KeyValueWriteConfig = {
    val writeMode = properties.get(KeyValueOptions.WriteMode) match {
      case null => None
      case KeyValueOptions.WriteModeReplace => Some(KeyValueOptions.WriteModeReplace)
      case v => 
        throw new IllegalArgumentException("Unknown KeyValueOptions.WriteMode option: " + v)
    }
    
    KeyValueWriteConfig(
      conf.implicitBucketNameOr(properties.get(KeyValueOptions.Bucket)),
      conf.implicitScopeNameOr(properties.get(KeyValueOptions.Scope)),
      conf.implicitCollectionName(properties.get(KeyValueOptions.Collection)),
      Option(properties.get(KeyValueOptions.IdFieldName))
        .getOrElse(DefaultConstants.DefaultIdFieldName),
      Option(properties.get(KeyValueOptions.CasFieldName)),
      Option(properties.get(KeyValueOptions.Durability)),
      Option(properties.get(KeyValueOptions.Timeout)),
      Option(properties.get(KeyValueOptions.ConnectionIdentifier)),
      Option(properties.get(KeyValueOptions.ErrorHandler)),
      Option(properties.get(KeyValueOptions.ErrorBucket)),
      Option(properties.get(KeyValueOptions.ErrorScope)),
      Option(properties.get(KeyValueOptions.ErrorCollection)),
      writeMode
    )
  }

  def streamConfig(properties: util.Map[String, String]): KeyValueStreamConfig = {
    val sparkContext = SparkSession.active.sparkContext

    val ci = Option(properties.get(KeyValueOptions.ConnectionIdentifier))
    val conf = CouchbaseConfig(
      sparkContext.getConf,
      ci
    )

    val defaultNumPartitions = sparkContext.defaultParallelism

    var collections: Seq[String] = Seq()

    val userCollection = conf.implicitCollectionName(properties.get(KeyValueOptions.Collection))
    if (userCollection.isDefined) {
      collections = collections :+ userCollection.get
    }
    val userCollections = conf.implicitCollectionName(properties.get(KeyValueOptions.Collections))
    if (userCollections.isDefined) {
      collections = collections ++ userCollections.get.split(",")
    }

    val meta         = properties.get(KeyValueOptions.StreamMetaData)
    val streamXattrs = meta != null && meta.equals(KeyValueOptions.StreamMetaDataFull)

    val streamFrom = properties.get(KeyValueOptions.StreamFrom) match {
      case null | KeyValueOptions.StreamFromNow => StreamFromVariants.FromNow
      case KeyValueOptions.StreamFromBeginning  => StreamFromVariants.FromBeginning
      case v =>
        throw new IllegalArgumentException("Unknown KeyValueOptions.StreamFrom option: " + v)
    }

    val c = KeyValueStreamConfig(
      streamFrom,
      Option(properties.get(KeyValueOptions.NumPartitions))
        .getOrElse(defaultNumPartitions.toString)
        .toInt,
      conf.implicitBucketNameOr(properties.get(KeyValueOptions.Bucket)),
      conf.implicitScopeNameOr(properties.get(KeyValueOptions.Scope)),
      collections,
      Option(properties.get(KeyValueOptions.StreamContent)).getOrElse("true").toBoolean,
      streamXattrs,
      Option(properties.get(KeyValueOptions.StreamFlowControlBufferSize)).map(_.toInt),
      Option(properties.get(KeyValueOptions.StreamPersistencePollingInterval)),
      ci
    )
    logDebug(s"Using KeyValueStreamConfig of $c")
    c
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val always =
      if (
        options.containsKey(KeyValueOptions.StreamContent) && !options
          .get(KeyValueOptions.StreamContent)
          .toBoolean
      ) {
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
      StructField("collection", StringType, nullable = true)
    )
    val full = Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("vbucket", IntegerType, nullable = false),
      StructField("xattrs", MapType(StringType, StringType), nullable = true)
    )

    val fields = options.get(KeyValueOptions.StreamMetaData) match {
      case KeyValueOptions.StreamMetaDataNone         => always
      case null | KeyValueOptions.StreamMetaDataBasic => always ++ basic
      case KeyValueOptions.StreamMetaDataFull         => always ++ basic ++ full
    }

    logDebug(s"Streaming the following fields $fields")

    StructType(fields)
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    new KeyValueTable(schema, streamConfig(properties))
  }

}

class RelationPartitionWriter(
    writeConfig: KeyValueWriteConfig,
    couchbaseConfig: CouchbaseConfig,
    mode: SaveMode
) extends ForeachPartitionFunction[String]
    with Logging {

  private val (errorHandler, errorDocumentHandler)
      : (Option[KeyValueWriteErrorHandler], Option[KeyValueErrorDocumentHandler]) = {
    val regularHandler = writeConfig.errorHandler.map(validateAndInstantiateCustomErrorHandler)
    val documentHandler =
      writeConfig.errorBucket.map(_ => new KeyValueErrorDocumentHandler(writeConfig, couchbaseConfig))
    (regularHandler, documentHandler)
  }

  // transient lazy defers this until it's run on the executor. Required as ErrorQueueManager is not serializable.
  @transient private lazy val errorQueueManager = new ErrorQueueManager(errorHandler, errorDocumentHandler)

  private def validateAndInstantiateCustomErrorHandler(
      className: String
  ): KeyValueWriteErrorHandler = {
    try {
      val clazz    = Class.forName(className)
      val instance = clazz.getDeclaredConstructor().newInstance()
      if (!instance.isInstanceOf[KeyValueWriteErrorHandler]) {
        throw new IllegalArgumentException(
          s"Error handler class $className does not implement KeyValueWriteErrorHandler"
        )
      }
      instance.asInstanceOf[KeyValueWriteErrorHandler]
    } catch {
      case _: ClassNotFoundException =>
        throw new IllegalArgumentException(s"Error handler class not found: $className")
      case _: InstantiationException =>
        throw new IllegalArgumentException(
          s"Error handler class $className cannot be instantiated (no default constructor?)"
        )
      case _: IllegalAccessException =>
        throw new IllegalArgumentException(
          s"Error handler class $className constructor is not accessible"
        )
      case e: IllegalArgumentException =>
        throw e // Re-throw our custom messages
      case e: Exception =>
        throw new IllegalArgumentException(
          s"Failed to validate error handler class $className: ${e.getMessage}",
          e
        )
    }
  }

  override def call(t: util.Iterator[String]): Unit = {
    try {
      val scopeName      = writeConfig.scope.getOrElse(DefaultConstants.DefaultScopeName)
      val collectionName = writeConfig.collection.getOrElse(DefaultConstants.DefaultCollectionName)

      logInfo("Starting JSON conversion of all documents")

      val keyValues = t.asScala
        .map(encoded => {
          val decoded = JsonObject.fromJson(encoded)
          val id = decoded.str(writeConfig.idFieldName)
          decoded.remove(writeConfig.idFieldName)

          val cas: Option[Long] = writeConfig.casFieldName match {
            case Some(casFieldName) =>
              decoded.safe.numLong(casFieldName) match {
                case Failure(_) =>
                  // Fail up front.  Usually if CAS is not present it's not going to be present for any doc, and we
                  // don't want to invoke the error handler pointlessly N times.
                  throw new IllegalArgumentException(s"CAS field '${casFieldName}' was specified but no valid CAS value found in that column for document '$id'")
                case Success(cas) =>
                  decoded.safe.remove(casFieldName)
                  Some(cas)
              }
            case _ => None
          }

          // A query read can now easily add this CAS field.
          // We'll assume the user doesn't want to write a column __META_CAS to their KV docs, regardless of whether
          // they have explicitly requested CAS or not.
          decoded.safe.remove(DefaultConstants.DefaultCasFieldName)

          (id, decoded, cas)
        })
        .toList

      logInfo("Completed JSON conversion of all documents")

      val coll = CouchbaseConnection(writeConfig.connectionIdentifier)
        .cluster(couchbaseConfig)
        .bucket(writeConfig.bucket)
        .scope(scopeName)
        .collection(collectionName)

      val durability = writeConfig.durability match {
        case Some(v) if v == KeyValueOptions.MajorityDurability => Durability.Majority
        case Some(v) if v == KeyValueOptions.MajorityAndPersistToActiveDurability =>
          Durability.MajorityAndPersistToActive
        case Some(v) if v == KeyValueOptions.PersistToMajorityDurability =>
          Durability.PersistToMajority
        case None => Durability.Disabled
        case d => throw new IllegalArgumentException("Unknown/Unsupported durability provided: " + d)
      }

      SFlux
        .fromIterable(keyValues)
        .flatMap(v => {
          val (id, decoded, cas) = v
          
          val content = decoded.toString
          val baseMono = writeConfig.writeMode match {
            case Some(KeyValueOptions.WriteModeReplace) =>
              cas match {
                case Some(casValue: Long) =>
                  coll.reactive.replace(id, content, buildReplaceOptions(durability, Some(casValue)))
                case _ =>
                  coll.reactive.replace(id, content, buildReplaceOptions(durability, None))
              }
              
            case _ =>
              val content = decoded.toString
              mode match {
                case SaveMode.Overwrite =>
                  coll.reactive.upsert(id, content, buildUpsertOptions(durability))
                case SaveMode.ErrorIfExists =>
                  coll.reactive.insert(id, content, buildInsertOptions(durability))
                case SaveMode.Ignore =>
                  coll.reactive.insert(id, content, buildInsertOptions(durability))
                case _ =>
                  throw new RuntimeException("Unexpected save mode " + mode)
              }
          }

          baseMono.onErrorResume(t => handleWriteError(t, id, mode))
        })
        .blockLast()
    } finally {
      errorQueueManager.shutdown()
    }
  }

  private def handleWriteError(
      throwable: Throwable,
      documentId: String,
      saveMode: SaveMode
  ): SMono[_] = {
    // Special case for SaveMode.Ignore: silently ignore DocumentExistsException
    if (saveMode == SaveMode.Ignore && throwable.isInstanceOf[DocumentExistsException]) {
      logInfo(s"Document $documentId already exists, ignoring as per SaveMode.Ignore")
      SMono.empty
    } else {
      if (errorHandler.isDefined || errorDocumentHandler.isDefined) {
        val errorInfo = createErrorInfo(documentId, throwable)
        // Intentionally not part of the reactive chain and will not block
        errorQueueManager.enqueueError(errorInfo)
        SMono.empty
      } else {
        SMono.error(throwable)
      }
    }
  }

  private def createErrorInfo(documentId: String, throwable: Throwable): KeyValueWriteErrorInfo = {
    KeyValueWriteErrorInfo(
      bucket = writeConfig.bucket,
      scope = writeConfig.scope.getOrElse(DefaultConstants.DefaultScopeName),
      collection = writeConfig.collection.getOrElse(DefaultConstants.DefaultCollectionName),
      documentId = documentId,
      throwable = Some(throwable)
    )
  }

  def buildInsertOptions(durability: Durability): InsertOptions = {
    var opts = InsertOptions().transcoder(RawJsonTranscoder.Instance).durability(durability)
    writeConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }

  def buildUpsertOptions(durability: Durability): UpsertOptions = {
    var opts = UpsertOptions().transcoder(RawJsonTranscoder.Instance).durability(durability)
    writeConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }

  def buildReplaceOptions(durability: Durability, cas: Option[Long] = None): ReplaceOptions = {
    var opts = ReplaceOptions().transcoder(RawJsonTranscoder.Instance).durability(durability)
    cas.foreach(casValue => opts = opts.cas(casValue))
    writeConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }

}
