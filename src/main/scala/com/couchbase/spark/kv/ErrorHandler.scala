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

import java.io.{BufferedWriter, FileOutputStream, FileWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.time.Instant
import com.couchbase.client.scala.json.JsonObject

/** Error handler that will receive any errors encountered by the Couchbase Spark Connector while
  * trying to write Key-Value DataFrame operations.
  *
  * Error handling is very application-dependent and users are encouraged to copy this code into
  * their own application and adapt to their needs. It should be seen as an example.
  *
  * It is recommended that users prefer using KeyValueOptions.ErrorBucket instead, which will write
  * errors to a specified collection on the Couchbase cluster. This is a much simpler and more
  * versatile form of error handling, as Apache Spark's architecture means this ErrorHandler class
  * does have some caveats that limit its usefulness:
  *
  * Where this executes
  * -------------------
  * It's very important to note that this will execute on the Sparker executor
  * (worker), and not inside the driver application. When doing initial testing with
  * `.master("local[*]")` these are one and the same. But when deploying the application to a Spark
  * cluster, the workers are usually separate nodes. Many things that will work fine in local
  * testing - like maintaining a list of received errors and processing them in the application -
  * will not work on a real Spark cluster. The ErrorHandler in the application will never execute.
  *
  * The critical point: There is no way for the application to programmatically get access to the
  * values that are passed to this handler. To over-simplify what Spark does - it will run your
  * application, and also copy your application to the executors and run it there. Each copy of that
  * application will have their own independent copy of this ErrorHandler, running on separate JVM
  * processes, generally on different nodes. The Spark Connector will be running key-value
  * operations on the Spark executors, and sending failures to the copies of ErrorHandler that
  * execute there.
  *
  * So ErrorHandler logs any failures and also appends them to a JSONL file, and it's crucial to
  * understand that all of this logic will execute on the Spark executor (worker). So logging will
  * appear in the Spark executor logs, and the file will be created on the Spark executor. It will
  * be up to the application to find and copy these files from the Spark executors later. Per above,
  * users may find KeyValueOptions.ErrorBucket a simpler solution - the application can simply fetch
  * any errors from Couchbase after the job.
  */
class ErrorHandler extends KeyValueWriteErrorHandler {
  override def onError(errorInfo: KeyValueWriteErrorInfo): Unit = {
    val documentId = errorInfo.documentId

    val errorClass = errorInfo.throwable match {
      case Some(t) => t.getClass.getSimpleName
      case _       => "Unknown"
    }

    val logMessage =
      s"ErrorHandler: Failure on document ${errorInfo.bucket}.${errorInfo.scope}.${errorInfo.collection} $documentId: " +
        s"${errorInfo.throwable.map(_.toString).getOrElse("Exception not available")}"

    // Reminder: all logging will appear in the Spark executor (worker) logs, not the primary application
    println(logMessage)

    // Create JSON representation of error
    val errorJson = JsonObject.create
      .put("documentId", documentId)
      .put("errorClass", errorClass)
      .put("bucket", errorInfo.bucket)
      .put("scope", errorInfo.scope)
      .put("collection", errorInfo.collection)
      .put("timestamp", Instant.now().toString)

    // Write to JSONL file (reminder: this will be on the Spark executor, not the primary application)
    try {
      val fileName = s"/tmp/couchbase-errors.jsonl"
      val writer = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(fileName, true), StandardCharsets.UTF_8)
      )
      try {
        writer.write(errorJson.toString)
        writer.newLine()
      } finally {
        writer.close()
      }
    } catch {
      case e: Exception =>
        println(s"ErrorHandler failed to write error to file: ${e.getMessage}")
    }
  }
}
