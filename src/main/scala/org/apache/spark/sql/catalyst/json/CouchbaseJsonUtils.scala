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

package org.apache.spark.sql.catalyst.json

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.spark.sql.catalyst.json.{JSONOptionsInRead, JacksonParser}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/** Internal utilities to access the JSON spark functionality.
  */
object CouchbaseJsonUtils {

  def jsonParser(schema: DataType): JacksonParser = {
    val options = new JSONOptionsInRead(Map.empty, SQLConf.get.sessionLocalTimeZone)
    new JacksonParser(schema, options, true)
  }

  def createParser(): (JsonFactory, String) => JsonParser = stringParser

  private def stringParser(jsonFactory: JsonFactory, record: String): JsonParser = {
    jsonFactory.createParser(record)
  }

}
