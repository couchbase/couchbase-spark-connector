/*
 * Copyright (c) 2024 Couchbase, Inc.
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
package com.couchbase.spark.util

import org.apache.spark.sql.SparkSession

/** Helpers for creating SparkSessions */
object SparkSessionHelper {
  def sparkSessionBuilder(): SparkSession.Builder = {
    SparkSession
      .builder()
      .master("local[*]")
  }

  def provideCouchbaseCreds(
      sparkBuilder: SparkSession.Builder,
      params: CouchbaseClusterSettings
  ): SparkSession.Builder = {
    sparkBuilder
      .config("spark.couchbase.connectionString", params.connectionString)
      .config("spark.couchbase.username", params.username)
      .config("spark.couchbase.password", params.password)
  }
}
