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

package org.apache.spark.sql

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptions, JSONOptionsInRead, JacksonGenerator, JacksonParser}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

import java.io.Writer

object CouchbaseJsonUtils {

  def jsonParser(schema: DataType): JacksonParser = {
    val options = new JSONOptionsInRead(Map.empty, SQLConf.get.sessionLocalTimeZone)
    new JacksonParser(schema, options, true)
  }

  def jsonGenerator(schema: DataType, writer: Writer): JacksonGenerator = {
    val options = new JSONOptions(Map.empty, SQLConf.get.sessionLocalTimeZone)
    new JacksonGenerator(schema, writer, options)
  }

  def createParser(): (JsonFactory, String) => JsonParser = CreateJacksonParser.string

}
