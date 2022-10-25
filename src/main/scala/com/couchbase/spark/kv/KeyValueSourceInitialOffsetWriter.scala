/*
 * Copyright (c) 2022 Couchbase, Inc.
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

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.HDFSMetadataLog
import org.json4s.{DefaultFormats, NoTypeHints}
import org.json4s.jackson.Serialization

import java.io.{BufferedWriter, InputStream, InputStreamReader, OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

class KeyValueSourceInitialOffsetWriter(sparkSession: SparkSession, metadataPath: String)
    extends HDFSMetadataLog[Map[Int, KeyValueStreamOffset]](sparkSession, metadataPath) {
  implicit val defaultFormats: DefaultFormats = DefaultFormats

  override protected def serialize(
      metadata: Map[Int, KeyValueStreamOffset],
      out: OutputStream
  ): Unit = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val serialized       = Serialization.write(metadata)

    val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
    writer.write(serialized)
    writer.flush()
  }

  override protected def deserialize(in: InputStream): Map[Int, KeyValueStreamOffset] = {
    val content = IOUtils.toString(new InputStreamReader(in, StandardCharsets.UTF_8))
    Serialization.read[Map[Int, KeyValueStreamOffset]](content)
  }
}
