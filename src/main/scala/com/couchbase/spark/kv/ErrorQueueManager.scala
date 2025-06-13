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
package com.couchbase.spark.kv

import org.apache.spark.internal.Logging

import java.util.concurrent.{BlockingQueue, Executors, LinkedBlockingQueue}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.util.{Failure, Success, Try}

/* Goals of this class are to balance:
 * Making the user-facing KeyValueWriteErrorHandler interface easy to use by removing concurrency as a concern.
 * Make errors not further slowdown the actual operations.
 * Not become overwhelmed by excessive errors (e.g. if every operation fails, the pathological case).
 * Not put excessive further load on the server in that pathological case.
 *
 * In cases where there are many errors it is better to prioritise server stability and not prolonging the Spark job
 * needlessly, rather than aim to guarantee delivery of every error.  Error handling should be regarded as best-effort.
 */
class ErrorQueueManager(
    customErrorHandler: Option[KeyValueWriteErrorHandler],
    documentErrorHandler: Option[KeyValueErrorDocumentHandler]
) extends Logging {

  private val MaxQueueSize = 10000
  private val queue: BlockingQueue[KeyValueWriteErrorInfo] =
    new LinkedBlockingQueue[KeyValueWriteErrorInfo](MaxQueueSize)
  private val shutdownFlag = new AtomicBoolean(false)
  private val errorCount = new AtomicInteger()
  private val executor = Executors.newSingleThreadExecutor(r => {
    val thread = new Thread(r, "couchbase-error-queue")
    thread.setDaemon(true)
    thread
  })

  // Start the processing thread.  For simplicity, error handling is currently single threaded.  In future
  // we could support parallelism (but not for the customErrorHandler case).
  executor.submit(new Runnable {
    override def run(): Unit = {
      while (!shutdownFlag.get() || !queue.isEmpty) {
        try {
          val errorInfo = queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS)
          if (errorInfo != null) {
            processError(errorInfo)
          }
        } catch {
          case ex: Exception =>
            logError("Unexpected error in error queue processing thread", ex)
            // Intentionally ignore and keep processing
        }
      }
    }
  })

  // This is non-blocking and will intentionally drop errors if required
  def enqueueError(errorInfo: KeyValueWriteErrorInfo): Unit = {
    val newErrorCount = errorCount.incrementAndGet()
    if (shutdownFlag.get()) {
      logWarning(s"Error queue is shut down, dropping error ${errorInfo}. Have seen ${newErrorCount} total errors")
    }
    else if (!queue.offer(errorInfo)) {
      logWarning(s"Error queue is full (${MaxQueueSize} items), dropping ${errorInfo}. Have seen ${newErrorCount} total errors")
    }
  }

  private def processError(errorInfo: KeyValueWriteErrorInfo): Unit = {
    // Call custom error handler first if configured
    customErrorHandler.foreach { handler =>
      Try(handler.onError(errorInfo)) match {
        case Success(_) =>
        case Failure(ex) =>
          logWarning(
            s"Custom error handler failed for document ${errorInfo.documentId}: ${ex.getMessage}"
          )
      }
    }

    documentErrorHandler.foreach { handler =>
      Try {
        handler.handleError(
          errorInfo.documentId,
          errorInfo.bucket,
          errorInfo.scope,
          errorInfo.collection,
          errorInfo.throwable.orNull
        )
      } match {
        case Success(_) =>
        case Failure(ex) =>
          logWarning(
            s"Document error handler failed for ${errorInfo}: ${ex.getMessage}"
          )
      }
    }
  }

  def shutdown(): Unit = {
    logInfo(s"Main operation has ended.  Have ${queue.size()} errors queued to process. Have seen ${errorCount} total errors")
    shutdownFlag.set(true)
    executor.shutdown()
    try {
      executor.awaitTermination(1, java.util.concurrent.TimeUnit.MINUTES)
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        executor.shutdownNow()
    }
    logInfo("Finished processing all errors")
  }
}
