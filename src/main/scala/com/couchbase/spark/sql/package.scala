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


import com.couchbase.client.java.document.JsonDocument
import com.couchbase.spark.rdd.QueryRDD
import org.apache.spark.sql._

package object sql {

  implicit def toDataFrameReaderFunctions(dfr: DataFrameReader): DataFrameReaderFunctions =
    new DataFrameReaderFunctions(dfr)

  implicit def toDataFrameWriterFunctions(dfw: DataFrameWriter[Object]): DataFrameWriterFunctions =
    new DataFrameWriterFunctions(dfw)

  implicit object Encoder {

    implicit def beanEncoder = Encoders.kryo(getClass)

    implicit def rowEncoder = Encoders.kryo(classOf[Row])

    implicit def queryEncoder = Encoders.kryo(classOf[QueryRDD])

    implicit def stringEncoder = Encoders.STRING

    implicit def jsonEncoder = Encoders.kryo(classOf[JsonDocument])
  }
}
