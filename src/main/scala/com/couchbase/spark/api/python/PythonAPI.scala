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
package com.couchbase.spark.api.python

import com.couchbase.client.java.document._
import com.couchbase.spark._


import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.concurrent.duration.Duration


/**
 * Functions available to be called from Python side
 */ 
class PythonAPI(){
  
  /**
   * Create duration option from timeout and it's unit.
   */
  private def timeoutAsDuration(timeout: Int = 0,
                                timeoutUnit: String = "millis") = {
    val duration = if (timeout != 0) Duration(timeout, timeoutUnit) else null
    Option(duration)
  }

  def couchbaseGet(sc: SparkContext, ids: Array[String], 
                   bucketName: String, numSlices: Int,
                   timeout: Int, timeoutUnit: String ) = {
    val nSlices = if (numSlices == 0) sc.defaultParallelism else numSlices
    sc.couchbaseGet[RawJsonDocument](ids, bucketName, nSlices,
                                     timeoutAsDuration(timeout, timeoutUnit))
  }
  

  def couchbaseGet(rdd: RDD[String], bucketName: String,
                   timeout: Int, timeoutUnit: String) = {
    rdd.couchbaseGet[RawJsonDocument](bucketName, 
                                      timeoutAsDuration(timeout, timeoutUnit)) 
  }

  def saveToCouchbase(rdd: RDD[RawJsonDocument], bucketName: String = null,
                      storeMode: String = "UPSERT",
                      timeout: Int, timeoutUnit: String) = {
    rdd.saveToCouchbase(bucketName, StoreMode.valueOf(storeMode),
                        timeoutAsDuration(timeout, timeoutUnit))
  }

  def couchbaseSubdocLookup(rdd: RDD[String], get: Array[String],
                            exists: Array[String], bucketName: String,
                            timeout: Int, timeoutUnit: String) = {
    rdd.couchbaseSubdocLookup(get, exists, bucketName,
                              timeoutAsDuration(timeout, timeoutUnit))
  }

  def couchbaseSubdocLookup(sc: SparkContext, 
                            ids: Array[String], get: Array[String],
                            exists: Array[String], bucketName: String,
                            timeout: Int, timeoutUnit: String) = {
    sc.couchbaseSubdocLookup(ids, get, exists, bucketName,
                             timeoutAsDuration(timeout, timeoutUnit))
  }

  /**
   * Serialize scala RDD
   */  
  def pickleRows(rdd: RDD[_]) = {
    rdd.mapPartitions(BatchPickler.pickle, true)
  }

  /**
   * Deserialize RDD
   */ 
  def unpickleRows(rdd: RDD[Array[Byte]]) = {
    rdd.flatMap(BatchUnpickler.unpickle)
  }

  /**
   * Convert scala RDD to Java to be used in python RDD
   */ 
  def javaRDD(rdd: RDD[_]) = JavaRDD.fromRDD(rdd)

}
