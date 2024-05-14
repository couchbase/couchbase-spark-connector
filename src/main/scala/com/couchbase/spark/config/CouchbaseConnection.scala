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

package com.couchbase.spark.config

import com.couchbase.client.core.config.PortInfo
import com.couchbase.client.core.env._
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.transaction.config.CoreTransactionsCleanupConfig
import com.couchbase.client.core.util.ConnectionString
import com.couchbase.client.scala.env.{ClusterEnvironment, SecurityConfig}
import com.couchbase.client.scala.{Bucket, Cluster, ClusterOptions, Collection, Scope}
import com.couchbase.spark.config.CouchbaseConnection.connections
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import org.apache.spark.internal.Logging
import reactor.core.publisher.Flux

import java.nio.file.Paths
import java.util
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.{Duration, DurationInt}

class CouchbaseConnection(val identifier: String) extends Serializable with Logging {

  @transient var envRef: Option[ClusterEnvironment] = None

  @transient var clusterRef: Option[Cluster] = None

  @transient var bucketsRef: mutable.Map[String, Bucket] = mutable.HashMap()

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      Thread.currentThread().setName(s"couchbase-shutdown-in-progress-$identifier")
      CouchbaseConnection(Some(identifier)).stop()
      Thread.currentThread().setName(s"couchbase-shutdown-complete-$identifier")
    }
  })

  def cluster(cfg: CouchbaseConfig): Cluster = {
    this.synchronized {
      if (envRef.isEmpty) {
        // The spark connector does not use any transaction functionality, so make use of the backdoor
        // to disable txn processing and save resources.
        System.setProperty(
          CoreTransactionsCleanupConfig.TRANSACTIONS_CLEANUP_LOST_PROPERTY,
          "false"
        )
        System.setProperty(
          CoreTransactionsCleanupConfig.TRANSACTIONS_CLEANUP_REGULAR_PROPERTY,
          "false"
        )

        var builder = ClusterEnvironment.builder
        
        val parsedConnstr = ConnectionString.create(cfg.connectionString)
        if (
          cfg.sparkSslOptions.enabled || parsedConnstr
            .scheme() == ConnectionString.Scheme.COUCHBASES
        ) {
          var securityConfig = SecurityConfig().enableTls(true)
          if (cfg.sparkSslOptions.keystorePath.isDefined) {
            securityConfig = securityConfig.trustStore(
              Paths.get(cfg.sparkSslOptions.keystorePath.get),
              cfg.sparkSslOptions.keystorePassword.get
            )
          } else if (cfg.sparkSslOptions.insecure) {
            securityConfig =
              securityConfig.trustManagerFactory(InsecureTrustManagerFactory.INSTANCE)
          }
          builder = builder.securityConfig(securityConfig)
        }
        builder = builder.loaders(Seq(new SparkPropertyLoader(cfg.properties)))

        envRef = Option(builder.build.get)
      }

      if (clusterRef.isEmpty) {
        val authenticator: Authenticator = {
          cfg.certAuthOptions.map(ca => CertificateAuthenticator.fromKeyStore(Paths.get(ca.keystorePath), ca.keystorePassword, Optional.of(ca.keystoreType)))
            .orElse(cfg.credentials.map(cr => PasswordAuthenticator.create(cr.username, cr.password))).get
        }

        clusterRef = Option(
          Cluster
            .connect(
              cfg.connectionString,
              ClusterOptions
                .create(authenticator)
                .environment(envRef.get)
            )
            .get
        )

        val waitUntilReadyTimeout =
          cfg.waitUntilReadyTimeout.map(s => Duration(s)).getOrElse(1.minutes)
        clusterRef.get.waitUntilReady(waitUntilReadyTimeout)
      }

      if (cfg.bucketName.isDefined) {
        // If an implicit bucket is defined, we proactively open the bucket also to help with pre 6.5 cluster
        // compatibility.
        val _ = bucket(cfg, None, clusterRef)
      }

      clusterRef.get
    }
  }

  def bucket(cfg: CouchbaseConfig, bucketName: Option[String]): Bucket = {
    bucket(cfg, bucketName, None)
  }

  private def bucket(
      cfg: CouchbaseConfig,
      bucketName: Option[String],
      c: Option[Cluster]
  ): Bucket = {
    val bname = this.bucketName(cfg, bucketName)
    this.synchronized {
      if (bucketsRef.contains(bname)) {
        return bucketsRef(bname)
      }
      val bucket = c match {
        case Some(cl) => cl.bucket(bname)
        case None     => cluster(cfg).bucket(bname)
      }
      bucketsRef.put(bname, bucket)

      val waitUntilReadyTimeout =
        cfg.waitUntilReadyTimeout.map(s => Duration(s)).getOrElse(1.minutes)
      bucket.waitUntilReady(waitUntilReadyTimeout)
      bucket
    }
  }

  def scope(cfg: CouchbaseConfig, bucketName: Option[String], scopeName: Option[String]): Scope = {
    val name = this.scopeName(cfg, scopeName)

    if (name.equals(CollectionIdentifier.DEFAULT_SCOPE)) {
      bucket(cfg, bucketName).defaultScope
    } else {
      bucket(cfg, bucketName).scope(name)
    }
  }

  def collection(
      cfg: CouchbaseConfig,
      bucketName: Option[String],
      scopeName: Option[String],
      collectionName: Option[String]
  ): Collection = {
    val parsedScopeName      = this.scopeName(cfg, scopeName)
    val parsedCollectionName = this.collectionName(cfg, collectionName)

    if (
      parsedScopeName.equals(CollectionIdentifier.DEFAULT_SCOPE)
      && parsedCollectionName.equals(CollectionIdentifier.DEFAULT_COLLECTION)
    ) {
      bucket(cfg, bucketName).defaultCollection
    } else {
      scope(cfg, bucketName, scopeName).collection(parsedCollectionName)
    }

  }

  def stop(): Unit = {
    this.synchronized {
      logInfo(s"Stopping CouchbaseConnection $identifier")
      try {
        if (clusterRef.isDefined) {
          clusterRef.get.disconnect()
          clusterRef = None
        }
        if (envRef.isDefined) {
          envRef.get.shutdown()
          envRef = None
        }
        connections.remove(identifier)
      } catch {
        case e: Throwable => logDebug(s"Encountered error during shutdown $e")
      }
    }
  }

  def bucketName(cfg: CouchbaseConfig, name: Option[String]): String = {
    name.orElse(cfg.bucketName) match {
      case Some(name) => name
      case None =>
        throw InvalidArgumentException
          .fromMessage(
            "No bucketName provided (neither configured globally, "
              + "nor in the per-command options)"
          )
    }
  }

  private def scopeName(cfg: CouchbaseConfig, name: Option[String]): String = {
    name.orElse(cfg.scopeName) match {
      case Some(name) => name
      case None       => CollectionIdentifier.DEFAULT_SCOPE
    }
  }

  private def collectionName(cfg: CouchbaseConfig, name: Option[String]): String = {
    name.orElse(cfg.collectionName) match {
      case Some(name) => name
      case None       => CollectionIdentifier.DEFAULT_COLLECTION
    }
  }

  def dcpSeedNodes(cfg: CouchbaseConfig, connectionIdentifier: Option[String]): String = {
    val core = CouchbaseConnection(connectionIdentifier)
      .cluster(cfg)
      .async
      .core
    val networkResolution = core.context.environment.ioConfig.networkResolution
    val tls = core.context.environment.securityConfig.tlsEnabled

    logDebug(s"Waiting for config, networkResolution=${networkResolution} tls=${tls}")

    // This is a hot stream that will return any config that's available.  Otherwise we'll block here
    // until one is.
    val nodes: Seq[PortInfo] = core.configurationProvider().configs
      // Wait for a non-empty config
      .filter(cc => cc.globalConfig() != null || (cc.bucketConfigs() != null && !cc.bucketConfigs().isEmpty))
      .flatMap(cc => {
        // If the cluster is not EOL then it should have provided a globalConfig via GCCCP.  As a backup we'll also
        // look for any bucket config.  All should contain at least one node we can use for DCP bootstrapping.
        if (cc.globalConfig != null) {
          Flux.fromIterable(cc.globalConfig.portInfos).collectList
        }
        else {
          // The filter check above guarantees we have a bucket config.
          val firstBucketConfig = cc.bucketConfigs.asScala.head._2
          Flux.fromIterable(firstBucketConfig.portInfos).collectList
        }
      })
      .doOnNext(v => logDebug(s"Got configuration with ${v.size} nodes"))
      // Safety timer on waiting for the config
      .blockFirst(core.context.environment.timeoutConfig.connectTimeout)
      .asScala

    val allSeedNodes = nodes
      .filter(node => {
        val nodeIsRunningKvService = node.ports().containsKey(ServiceType.KV) || node.sslPorts().containsKey(ServiceType.KV)
        if (!nodeIsRunningKvService) {
          logDebug(s"Filtering out non-KV node ${node} from DCP connection string")
        }
        nodeIsRunningKvService
      })
      .map(node => {
        var port = Option(node.ports().get(ServiceType.KV))
        var sslPort = Option(node.sslPorts().get(ServiceType.KV))
        var hostname = node.hostname

        // Get the resolved network name from the core context.
        // Empty optional means default (internal) network.
        // We know the alternate address has been resolved,
        // because DefaultConfigurationProvider has already pushed at least one config.
        core.context().alternateAddress().ifPresent(networkName => {
          val alternate = node.alternateAddresses().get(networkName);

          if (alternate != null) {
            port = Option(alternate.services.get(ServiceType.KV))
            sslPort = Option(alternate.sslServices.get(ServiceType.KV))
            hostname = alternate.hostname

            logDebug(s"Using alternate for network ${networkName} node ${node.hostname}: ${alternate.hostname} port=${port} sslPort=${sslPort}")
          }
        })

        val portToUse = if (tls) sslPort else port
        hostname + portToUse.map(p => ":" + p).getOrElse("")
      })

    val out = allSeedNodes.mkString(",")

    logDebug(s"Generated DCP string '${out}'")

    out
  }

  def dcpSecurityConfig(
      cfg: CouchbaseConfig,
      connectionIdentifier: Option[String]
  ): com.couchbase.client.dcp.SecurityConfig = {
    // Make sure we are bootstrapped.
    val _ = CouchbaseConnection(connectionIdentifier).cluster(cfg)

    val coreSecurityConfig = envRef.get.core.securityConfig()

    val dcpSecurityConfig = com.couchbase.client.dcp.SecurityConfig.builder()

    dcpSecurityConfig.enableTls(coreSecurityConfig.tlsEnabled())
    dcpSecurityConfig.enableNativeTls(coreSecurityConfig.nativeTlsEnabled())
    dcpSecurityConfig.enableHostnameVerification(coreSecurityConfig.hostnameVerificationEnabled())

    if (coreSecurityConfig.trustManagerFactory() != null) {
      dcpSecurityConfig.trustManagerFactory(coreSecurityConfig.trustManagerFactory())
    }
    if (coreSecurityConfig.trustCertificates() != null) {
      dcpSecurityConfig.trustCertificates(coreSecurityConfig.trustCertificates())
    }

    dcpSecurityConfig.build()
  }

}

object CouchbaseConnection {

  private val connections = new ConcurrentHashMap[String, CouchbaseConnection]()

  def apply(connectionIdentifier: Option[String] = None): CouchbaseConnection = {
    val i = connectionIdentifier.getOrElse("default")
    connections.computeIfAbsent(
      i,
      i => {
        new CouchbaseConnection(i)
      }
    )
  }

  private[couchbase] def stopAll(): Unit = {
    connections.values.forEach(c => c.stop())
  }
}

class SparkPropertyLoader(properties: Seq[(String, String)])
    extends AbstractMapPropertyLoader[CoreEnvironment.Builder[_]] {
  override def propertyMap(): util.Map[String, String] = {
    properties.toMap.asJava
  }
}
