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

import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by daschl on 27/05/16.
  */
object GenericSample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DatasetSample")
      .set("com.couchbase.bucket.travel-sample", "")

    val sc = new SparkContext(conf)

    // SparkConfig is not serializable, but the couchbase config is!
    val cbConf = CouchbaseConfig(conf)

    sc
      .parallelize(Seq("airline_10123"))
      .map(id => {
        // Grab the bucket reference and perform ops on it
        // !! MAKE SURE YOUR RETURN VALUE IS SERIAZLIABLE !!
        val bucket = CouchbaseConnection().bucket(cbConf, "travel-sample")
        bucket.lookupIn(id).get("name").execute().content("name")
      })
      .foreach(println)
  }

}
