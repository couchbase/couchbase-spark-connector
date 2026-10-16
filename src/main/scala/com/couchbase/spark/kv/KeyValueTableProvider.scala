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
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.codec.RawJsonTranscoder
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.json.{JsonObject}
import com.couchbase.client.scala.kv.{
  InsertOptions,
  MutateInOptions,
  ReplaceOptions,
  StoreSemantics,
  UpsertOptions
}
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.TaskContext
import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.types.{
  BinaryType,
  BooleanType,
  IntegerType,
  LongType,
  MapType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._
import java.util
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

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

    val writeModeOpt = Option(parameters.get(KeyValueOptions.WriteMode))
    val isSubdocMode = writeModeOpt.flatten.exists(m =>
      m == KeyValueOptions.WriteModeSubdocInsert ||
        m == KeyValueOptions.WriteModeSubdocReplace ||
        m == KeyValueOptions.WriteModeSubdocUpsert
    )
    if (isSubdocMode) {
      val idFieldName =
        parameters.getOrElse(KeyValueOptions.IdFieldName, DefaultConstants.DefaultIdFieldName)
      val casFieldOpt = parameters.get(KeyValueOptions.CasFieldName)
      SubdocUtil.validate(
        data.schema,
        idFieldName,
        casFieldOpt
      )
    }
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
    val writeMode = Option(properties.get(KeyValueOptions.WriteMode)) match {
      case None                                   => None
      case Some(KeyValueOptions.WriteModeReplace) => Some(KeyValueOptions.WriteModeReplace)
      case Some(KeyValueOptions.WriteModeSubdocInsert) =>
        Some(KeyValueOptions.WriteModeSubdocInsert)
      case Some(KeyValueOptions.WriteModeSubdocReplace) =>
        Some(KeyValueOptions.WriteModeSubdocReplace)
      case Some(KeyValueOptions.WriteModeSubdocUpsert) =>
        Some(KeyValueOptions.WriteModeSubdocUpsert)
      case Some(v) =>
        throw new IllegalArgumentException("Unknown KeyValueOptions.WriteMode option: " + v)
    }

    val maxConcurrentOps = Option(properties.get(KeyValueOptions.MaxConcurrentOps)) match {
      case None => RelationPartitionWriter.DefaultMaxConcurrentOps
      case Some(v) =>
        v.trim.toIntOption.filter(_ > 0).getOrElse(
          throw new IllegalArgumentException(
            s"KeyValueOptions.MaxConcurrentOps must be a positive integer, but was: $v"
          )
        )
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
      writeMode,
      maxConcurrentOps
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

// Each Spark task deserializes its own copy of this class.
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
      writeConfig.errorBucket.map(_ =>
        new KeyValueErrorDocumentHandler(writeConfig, couchbaseConfig)
      )
    (regularHandler, documentHandler)
  }

  private val loggedSubdocSpecs = new AtomicBoolean(false)

  // transient lazy defers this until it's run on the executor. Required as ErrorQueueManager is not serializable.
  @transient private lazy val errorQueueManager =
    new ErrorQueueManager(
      errorHandler,
      errorDocumentHandler,
      RelationPartitionWriter.partitionLabel(TaskContext.get())
    )

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
    // Force creation here on the task thread, as it labels itself from Spark's ThreadLocal TaskContext.
    errorQueueManager

    val partitionId = Option(TaskContext.get()).map(_.partitionId()).getOrElse(-1)
    val partition   = RelationPartitionWriter.partitionLabel(TaskContext.get())
    logInfo(s"Partition ${partition}: KV dataframe write starting")

    val stats = new PartitionWriteStats(partition, s"couchbase-kv-stats-partition-$partitionId")

    try {
      val scopeName      = writeConfig.scope.getOrElse(DefaultConstants.DefaultScopeName)
      val collectionName = writeConfig.collection.getOrElse(DefaultConstants.DefaultCollectionName)

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
        case d =>
          throw new IllegalArgumentException("Unknown/Unsupported durability provided: " + d)
      }

      logInfo(s"Partition ${partition}: KV dataframe write using config ${writeConfig} saveMode ${mode}")

      stats.start()

      val count = RelationPartitionWriter.writeAll(
        t.asScala,
        writeConfig.maxConcurrentOps,
        i => s"couchbase-kv-writer-partition-$partitionId-$i",
        stats
      ) { encoded =>
        write(coll, durability, encoded, stats)
      }

      logInfo(s"Partition ${partition}: KV dataframe write completed processing $count documents")
    } catch {
      case err: Throwable =>
        logError(
          s"Partition ${partition}: KV dataframe write failed due to " +
            PartitionWriteStats.describe(err)
        )
        // Must be rethrown, so that Spark fails (and retries) the task.
        throw err
    } finally {
      logInfo(s"Partition ${partition}: KV dataframe write finishing")
      stats.close()
      errorQueueManager.shutdown()
    }
  }

  private def toKeyValue(encoded: String): (String, JsonObject, Option[Long]) = {
    val decoded = JsonObject.fromJson(encoded)
    val id      = decoded.str(writeConfig.idFieldName)
    decoded.remove(writeConfig.idFieldName)

    val cas: Option[Long] = writeConfig.casFieldName match {
      case Some(casFieldName) =>
        decoded.safe.numLong(casFieldName) match {
          case Failure(_) =>
            // Fail the job rather than invoking the error handler.  Usually if CAS is not present it's not
            // going to be present for any doc, and we don't want to invoke the error handler pointlessly N
            // times.  Note that documents earlier in the partition may already have been written.
            throw new IllegalArgumentException(
              s"CAS field '${casFieldName}' was specified but no valid CAS value found in that column for document '$id'"
            )
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
  }

  private def write(
      coll: Collection,
      durability: Durability,
      encoded: String,
      stats: PartitionWriteStats
  ): Unit = {
    val conversionStarted  = System.nanoTime()
    val (id, decoded, cas) = toKeyValue(encoded)

    val op: () => Try[_] = writeConfig.writeMode match {
      case Some(KeyValueOptions.WriteModeSubdocInsert) |
          Some(KeyValueOptions.WriteModeSubdocReplace) |
          Some(KeyValueOptions.WriteModeSubdocUpsert) => {
        val (specs, debugInfo) = SubdocUtil.buildSubdocSpecs(decoded)
        val storeSemantics = writeConfig.writeMode.get match {
          case KeyValueOptions.WriteModeSubdocInsert  => StoreSemantics.Insert
          case KeyValueOptions.WriteModeSubdocReplace => StoreSemantics.Replace
          case _                                      => StoreSemantics.Upsert
        }

        // Log only the first to avoid spam.  The specs come from the column names so are usually the same for every document.
        if (loggedSubdocSpecs.compareAndSet(false, true)) {
          logInfo(
            s"Partition ${stats.partition}: performing subdoc mutations with ${storeSemantics}, " +
              s"specs for the first document: ${debugInfo}"
          )
        }

        val opts = buildMutateInOptions(durability, storeSemantics, cas)
        () => coll.mutateIn(id, specs, opts)
      }

      case _ =>
        val content = decoded.toString
        (writeConfig.writeMode, mode) match {
          case (Some(KeyValueOptions.WriteModeReplace), _) =>
            val opts = buildReplaceOptions(durability, cas)
            () => coll.replace(id, content, opts)
          case (_, SaveMode.Overwrite) =>
            val opts = buildUpsertOptions(durability)
            () => coll.upsert(id, content, opts)
          case (_, SaveMode.ErrorIfExists | SaveMode.Ignore) =>
            val opts = buildInsertOptions(durability)
            () => coll.insert(id, content, opts)
          case _ =>
            throw new RuntimeException("Unexpected save mode " + mode)
        }
    }

    val opStarted = System.nanoTime()
    stats.recordConversion(opStarted - conversionStarted)

    // Execute the KV op here.
    op() match {
      case Success(_) => stats.recordSuccess(System.nanoTime() - opStarted)
      case Failure(t) => handleWriteError(t, id, mode, stats)
    }
  }

  private def handleWriteError(
      throwable: Throwable,
      documentId: String,
      saveMode: SaveMode,
      stats: PartitionWriteStats
  ): Unit = {
    // Special case for SaveMode.Ignore: silently ignore DocumentExistsException
    if (saveMode == SaveMode.Ignore && throwable.isInstanceOf[DocumentExistsException]) {
      stats.recordIgnored()
      logInfo(s"Document $documentId already exists, ignoring as per SaveMode.Ignore")
    } else {
      stats.recordFailure(documentId, throwable)
      if (errorHandler.isDefined || errorDocumentHandler.isDefined) {
        val errorInfo = createErrorInfo(documentId, throwable)
        // Will not block
        errorQueueManager.enqueueError(errorInfo)
      } else {
        throw throwable
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

  def buildMutateInOptions(
      durability: Durability,
      storeSemantics: StoreSemantics,
      cas: Option[Long]
  ): MutateInOptions = {
    var opts = MutateInOptions().durability(durability).document(storeSemantics)
    cas.foreach(casValue => opts = opts.cas(casValue))
    writeConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }

}

object RelationPartitionWriter extends Logging {

  /** The default for how many KV operations each partition - and so each Spark task - keeps
    * in-flight at once. Can be overridden with [[KeyValueOptions.MaxConcurrentOps]].
    */
  val DefaultMaxConcurrentOps: Int = 16

  /** Identifies which Spark task a log line came from, as "3 of 128".
    *
    * The count is taken from the TaskContext rather than passed down from the driver, as under AQE
    * the number of partitions actually executed can differ from the number the driver planned.
    */
  private[kv] def partitionLabel(ctx: TaskContext): String =
    if (ctx == null) "unknown" else s"${ctx.partitionId()} of ${ctx.numPartitions()}"

  /** SPARKC-240: Read and convert documents while we stream
    * Runs `process` on each item using `numThreads` threads, and returns how many items were
    * processed.
    *
    * `items` is only ever advanced on the calling thread - the Spark task thread. Spark's iterator
    * may drive upstream work (reading files, fetching shuffle blocks, ...) that expects the task's
    * thread-local TaskContext, so it must not be advanced from any other thread. Items are handed
    * to the threads through a small bounded queue, so only around `2 * numThreads` items are held
    * in memory at once.
    *
    * The first exception thrown by `process` (or by `items`) fails the whole write. No further
    * items are processed, those already being processed are allowed to finish, and then the
    * exception is rethrown.
    */
  private[kv] def writeAll[T <: AnyRef](
      items: Iterator[T],
      numThreads: Int,
      threadName: Int => String,
      stats: PartitionWriteStats
  )(process: T => Unit): Long = {
    val partition  = stats.partition
    val queue      = new ArrayBlockingQueue[T](numThreads)
    val firstError = new AtomicReference[Throwable]()

    @volatile var producerDone = false

    def fail(e: Throwable, context: String): Unit =
      if (firstError.compareAndSet(null, e)) {
        logError(
          s"Partition $partition: $context, so the write will fail and remaining items will be " +
            s"skipped: ${PartitionWriteStats.describe(e)}"
        )
      }

    val threads = mutable.ArrayBuffer.empty[Thread]
    var count   = 0L
    try {
      for (i <- 0 until numThreads) {
        val thread = new Thread(
          () => {
            var processed = 0L
            // A worker exits when no more work can arrive, which is triggered by producerDone (the incoming items
            // stream has been fully consumed), and the work queue being empty.
            while (!(producerDone && queue.isEmpty)) {
              val item = queue.poll(100, TimeUnit.MILLISECONDS)
              // After a failure keep draining the queue, so the producer cannot block on it,
              // but skip the work.
              if (item != null && firstError.get() == null) {
                try process(item)
                // The first thrown error triggers task failure as is the normal Spark model.  Spark by default will
                // retry the full task (partition) 4 times.  It is now recommended that users apply the failure handling to have better handling
                // of e.g. occasional KV timeouts:
                // https://docs.couchbase.com/spark-connector/current/spark-sql.html#failure-handling-during-persistence
                catch { case e: Throwable => fail(e, s"worker ${Thread.currentThread().getName} failed") }
                processed += 1
              }
            }
            logInfo(
              s"Partition $partition: worker ${Thread.currentThread().getName} finished after " +
                s"processing $processed items"
            )
          },
          threadName(i)
        )
        thread.setDaemon(true)
        thread.start()
        threads += thread
      }

      // hasNext() as well as next() is timed, as Spark iterators often do the upstream work in hasNext().
      var readStarted = System.nanoTime()
      while (firstError.get() == null && items.hasNext) {
        // Each call to .next() here is executing the upstream Spark code, e.g. it may block as it reads from file etc.
        // The aim is to keep the queue ahead of the workers by ~numThreads items, which should ensure a smooth flow.
        val item = items.next()
        stats.recordUpstreamRead(System.nanoTime() - readStarted, queue.size())
        queue.put(item)
        count += 1
        readStarted = System.nanoTime()
      }
    } catch {
      case e: Throwable => fail(e, "reading the next item from Spark failed")
    } finally {
      if (firstError.get() == null) {
        logInfo(
          s"Partition $partition: finished reading all $count items from Spark, " +
            s"${queue.size()} are still queued.  Waiting for ${threads.size} worker threads to finish"
        )
      } else {
        logInfo(
          s"Partition $partition: stopped reading from Spark after $count items due to an error.  " +
            s"Waiting for ${threads.size} worker threads to finish their in-flight items"
        )
      }

      producerDone = true
      val joinStarted    = System.nanoTime()
      var lastWaitingLog = joinStarted
      var interrupted    = false
      threads.foreach { thread =>
        while (thread.isAlive) {
          try thread.join(1000)
          // If Spark kills the task, it interrupts the task thread.  We handle that here and let the worker threads
          // finish cleanly first (recording the interrupt as the error makes them skip any remaining queued items).
          // We will re-interrupt after all workers are done. The while loop also ensures that we still join the
          // interrupted worker.
          catch {
            case e: InterruptedException =>
              interrupted = true
              fail(e, "interrupted while waiting for worker threads (likely the task was killed) - will continue")
          }

          val now = System.nanoTime()
          if (now - lastWaitingLog >= PartitionWriteStats.DefaultInterval.toNanos) {
            lastWaitingLog = now
            val alive = threads.filter(_.isAlive)
            if (alive.nonEmpty) {
              logInfo(
                s"Partition $partition: still waiting after ${(now - joinStarted) / 1000000}ms " +
                  s"for ${alive.size} worker threads: ${alive.map(_.getName).mkString(", ")}"
              )
            }
          }
        }
      }
      logInfo(
        s"Partition $partition: all ${threads.size} worker threads finished, after waiting " +
          s"${(System.nanoTime() - joinStarted) / 1000000}ms"
      )
      if (interrupted) Thread.currentThread().interrupt()
    }

    val e = firstError.get()
    if (e != null) throw e
    count
  }
}
