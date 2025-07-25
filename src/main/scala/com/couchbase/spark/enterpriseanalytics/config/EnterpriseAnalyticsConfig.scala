/*
 * Copyright (c) 2025 Couchbase, Inc.
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

package com.couchbase.spark.enterpriseanalytics.config

import org.apache.spark.SparkConf

case class EnterpriseAnalyticsCredentials(username: String, password: String)

case class EnterpriseAnalyticsSparkSslOptions(insecure: Boolean)

case class EnterpriseAnalyticsConfig(
    connectionString: String,
    credentials: EnterpriseAnalyticsCredentials,
    sparkSslOptions: EnterpriseAnalyticsSparkSslOptions
)

object EnterpriseAnalyticsConfig {

  private val SPARK_PREFIX = "spark."

  private val PREFIX           = SPARK_PREFIX + "couchbase."
  private val Username         = PREFIX + "username"
  private val Password         = PREFIX + "password"
  private val ConnectionString = PREFIX + "connectionString"

  private val SPARK_SSL_PREFIX   = SPARK_PREFIX + "ssl."
  private val SPARK_SSL_ENABLED  = SPARK_SSL_PREFIX + "enabled"
  private val SPARK_SSL_INSECURE = SPARK_SSL_PREFIX + "insecure"

  def requireOption(cfg: SparkConf, key: String): String = {
    cfg.getOption(key) match {
      case Some(value) => value
      case None        => throw new IllegalArgumentException(s"Option '${key}' must be provided")
    }
  }

  def apply(cfg: SparkConf): EnterpriseAnalyticsConfig = {
    val connectionString = requireOption(cfg, ConnectionString)
    val username         = requireOption(cfg, Username)
    val password         = requireOption(cfg, Password)
    val credentials      = EnterpriseAnalyticsCredentials(username, password)

    cfg.getOption(SPARK_SSL_ENABLED) match {
      case Some(value) =>
        if (!value.toBoolean) {
          throw new IllegalArgumentException(
            s"${SPARK_SSL_ENABLED} is set to false, but SSL is mandatory for Enterprise Analytics connections"
          )
        }
      case None =>
    }

    val sslInsecure = cfg
      .getOption(SPARK_SSL_INSECURE)
      .getOrElse("false")
      .toBoolean

    EnterpriseAnalyticsConfig(
      connectionString,
      credentials,
      EnterpriseAnalyticsSparkSslOptions(insecure = sslInsecure)
    )
  }
}
