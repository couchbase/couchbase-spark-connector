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
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.driver.extraJavaOptions", 
        "--add-opens=java.base/java.lang=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
        "--add-opens=java.base/java.io=ALL-UNNAMED " +
        "--add-opens=java.base/java.net=ALL-UNNAMED " +
        "--add-opens=java.base/java.nio=ALL-UNNAMED " +
        "--add-opens=java.base/java.util=ALL-UNNAMED " +
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED " +
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
      )
      .config("spark.executor.extraJavaOptions", 
        "--add-opens=java.base/java.lang=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
        "--add-opens=java.base/java.io=ALL-UNNAMED " +
        "--add-opens=java.base/java.net=ALL-UNNAMED " +
        "--add-opens=java.base/java.nio=ALL-UNNAMED " +
        "--add-opens=java.base/java.util=ALL-UNNAMED " +
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED " +
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
      )
  }

  def provideCouchbaseCreds(
      sparkBuilder: SparkSession.Builder,
      params: CouchbaseClusterSettings
  ): SparkSession.Builder = {
    sparkBuilder
      .config("spark.couchbase.connectionString", params.connectionString)
      .config("spark.couchbase.username", params.username)
      .config("spark.couchbase.password", params.password)

    if (params.tlsInsecure) {
      sparkBuilder.config("spark.ssl.insecure", params.tlsInsecure.toString)
    }

    sparkBuilder
  }
}
