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

import com.couchbase.analytics.client.java.{Cluster, ClusterOptions, Credential}
import org.apache.spark.internal.Logging

import java.util.concurrent.ConcurrentHashMap

class EnterpriseAnalyticsConnection() extends Serializable with Logging {

  @transient var clusterRef: Option[Cluster] = None

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      Thread.currentThread().setName(s"couchbase-shutdown-in-progress")
      EnterpriseAnalyticsConnection().stop()
      Thread.currentThread().setName(s"couchbase-shutdown-complete")
    }
  })

  def cluster(cfg: EnterpriseAnalyticsConfig): Cluster = {
    this.synchronized {
      val creds = cfg.credentials

      clusterRef = Option(
        Cluster
          .newInstance(
            cfg.connectionString,
            Credential.of(creds.username, creds.password),
            (clusterOptions: ClusterOptions) => {
              clusterOptions.security(securityConfig => {
                if (cfg.sparkSslOptions.insecure) {
                  securityConfig.disableServerCertificateVerification(true)
                }
              })
            }
          )
      )

      clusterRef.get
    }
  }

  def stop(): Unit = {
    this.synchronized {
      logInfo(s"Stopping EnterpriseAnalyticsConnection")
      try {
        if (clusterRef.isDefined) {
          clusterRef.get.close()
          clusterRef = None
        }
        EnterpriseAnalyticsConnection.connections.remove("default")
      } catch {
        case e: Throwable => logDebug(s"Encountered error during shutdown $e")
      }
    }
  }
}

object EnterpriseAnalyticsConnection {

  private val connections = new ConcurrentHashMap[String, EnterpriseAnalyticsConnection]()

  def apply(): EnterpriseAnalyticsConnection = {
    // We only support a single connection today, but leaving door open to future
    val i = "default"
    connections.computeIfAbsent(
      i,
      i => {
        new EnterpriseAnalyticsConnection()
      }
    )
  }

  private[couchbase] def stopAll(): Unit = {
    connections.values.forEach(c => c.stop())
  }
}
