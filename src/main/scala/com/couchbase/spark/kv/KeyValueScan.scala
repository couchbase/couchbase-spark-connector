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

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.types.StructType

class KeyValueScan(schema: StructType, config: KeyValueStreamConfig) extends Scan {

  override def readSchema(): StructType = schema

  override def toContinuousStream(checkpointLocation: String): ContinuousStream =
    new KeyValueContinuousStream(schema, config, checkpointLocation)

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
    new KeyValueMicroBatchStream(schema, config, checkpointLocation)

}
