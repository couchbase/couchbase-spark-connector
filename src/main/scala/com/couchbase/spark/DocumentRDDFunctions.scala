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
package com.couchbase.spark

import com.couchbase.client.java.document.Document
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.internal.OnceIterable
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import rx.lang.scala.Observable
import rx.lang.scala.JavaConversions._

class DocumentRDDFunctions[D <: Document[_]](rdd: RDD[D])
  extends Serializable
  with Logging {

  private val cbConfig = CouchbaseConfig(rdd.context.getConf)

  def saveToCouchbase(bucketName: String = null, storeMode: StoreMode = StoreMode.UPSERT): Unit = {
    rdd.foreachPartition(iter => {
      if (iter.nonEmpty) {
        val bucket = CouchbaseConnection().bucket(cbConfig, bucketName).async()
        Observable
          .from(OnceIterable(iter))
          .flatMap(doc =>  {
            storeMode match {
              case StoreMode.UPSERT => toScalaObservable(bucket.upsert[D](doc))
              case StoreMode.INSERT_AND_FAIL => toScalaObservable(bucket.insert[D](doc))
              case StoreMode.REPLACE_AND_FAIL => toScalaObservable(bucket.replace[D](doc))
              case StoreMode.INSERT_AND_IGNORE => toScalaObservable(bucket.insert[D](doc))
                .doOnError(err => logWarning("Insert failed, but suppressed.", err))
                .onErrorResumeNext(throwable => Observable.empty)
              case StoreMode.REPLACE_AND_IGNORE => toScalaObservable(bucket.replace[D](doc))
                .doOnError(err => logWarning("Replace failed, but suppressed.", err))
                .onErrorResumeNext(throwable => Observable.empty)
            }
          })
          .toBlocking
          .last
      }
    })
  }

}
