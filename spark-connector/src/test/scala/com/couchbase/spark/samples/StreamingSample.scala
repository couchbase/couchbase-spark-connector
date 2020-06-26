/*
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.spark.samples

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.couchbase.spark.streaming._
import org.apache.spark.storage.StorageLevel


/**
  * Class to manually run and test the spark streaming. This is not intended as a integration test.
  */
object StreamingSample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("StreamingSample")
      .set("com.couchbase.username", "Administrator")
      .set("com.couchbase.password", "password")
      .set("com.couchbase.bucket.beer-sample", "")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc
      .couchbaseStream(from = FromBeginning, to = ToNow, storageLevel = StorageLevel.MEMORY_ONLY)
      .map(_.getClass)
      .countByValue()
      .print()

    ssc.start()
    ssc.awaitTermination()
  }

}
