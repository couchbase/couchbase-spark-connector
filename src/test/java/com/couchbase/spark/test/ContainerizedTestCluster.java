/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.spark.test;

// CHECKSTYLE:OFF IllegalImport - Allow unbundled Jackson

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.shaded.okhttp3.Credentials;
import org.testcontainers.shaded.okhttp3.FormBody;
import org.testcontainers.shaded.okhttp3.OkHttpClient;
import org.testcontainers.shaded.okhttp3.Request;
import org.testcontainers.shaded.okhttp3.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Uses containers to manage the test cluster (by using testcontainers).
 *
 * @since 2.0.0
 */
public class ContainerizedTestCluster extends TestCluster {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerizedTestCluster.class);

  private final OkHttpClient httpClient = new OkHttpClient.Builder().build();

  private final SupportedVersion version;
  private final int numNodes;
  private final String adminUsername;
  private final String adminPassword;
  private volatile String bucketname;
  private final List<CouchbaseContainer> containers = new CopyOnWriteArrayList<>();


  ContainerizedTestCluster(final Properties properties) {
    version = SupportedVersion.fromString(properties.getProperty("cluster.containerized.version"));
    numNodes = Integer.parseInt(properties.getProperty("cluster.containerized.numNodes"));
    adminUsername = properties.getProperty("cluster.adminUsername");
    adminPassword = properties.getProperty("cluster.adminPassword");
  }

  @Override
  ClusterType type() {
    return ClusterType.CONTAINERIZED;
  }

  @Override
  TestClusterConfig _start() throws Exception {
    LOGGER.info("Starting Containerized Cluster of {} nodes", numNodes);
    long start = System.nanoTime();
    for (int i = 0; i < numNodes; i++) {
      CouchbaseContainer container = new CouchbaseContainer(version);
      containers.add(container);
      container.start();
    }
    long end = System.nanoTime();
    LOGGER.debug("Starting Containerized Cluster took {}s", TimeUnit.NANOSECONDS.toSeconds(end - start));

    String seedHost = containers.get(0).getContainerIpAddress();
    int seedPort = containers.get(0).getMappedPort(CouchbaseContainer.CouchbasePort.REST);

    initFirstNode(containers.get(0), seedHost, seedPort);
    // todo: then add all others with services to the cluster
    // todo: then rebalance

    bucketname = UUID.randomUUID().toString();

    Response postResponse = httpClient.newCall(new Request.Builder()
      .header("Authorization", Credentials.basic(adminUsername, adminPassword))
      .url("http://" + seedHost + ":" + seedPort + "/pools/default/buckets")
      .post(new FormBody.Builder()
        .add("name", bucketname)
        .add("ramQuotaMB", "100")
        .build())
      .build())
      .execute();

    if (postResponse.code() != 202) {
      throw new Exception("Could not create bucket: " + postResponse);
    }

    Response getResponse = httpClient.newCall(new Request.Builder()
      .header("Authorization", Credentials.basic(adminUsername, adminPassword))
      .url("http://" + seedHost + ":" + seedPort + "/pools/default/b/" + bucketname)
      .build())
      .execute();

    String raw = getResponse.body().string();

    Response getClusterVersionResponse = httpClient.newCall(new Request.Builder()
            .header("Authorization", Credentials.basic(adminUsername, adminPassword))
            .url("http://" + seedHost + ":" + seedPort + "/pools")
            .build())
            .execute();

    ClusterVersion clusterVersion = parseClusterVersion(getClusterVersionResponse);

    return new TestClusterConfig(
      bucketname,
      adminUsername,
      adminPassword,
      nodesFromRaw(seedHost, raw),
      replicasFromRaw(raw),
      loadClusterCertificate(seedHost, seedPort),
      capabilitiesFromRaw(raw),
      clusterVersion
    );
  }

  private Optional<X509Certificate> loadClusterCertificate(String seedHost, int seedPort) throws Exception {
    Response getResponse = httpClient.newCall(new Request.Builder()
      .header("Authorization", Credentials.basic(adminUsername, adminPassword))
      .url("http://" + seedHost + ":" + seedPort + "/pools/default/certificate")
      .build())
      .execute();

    String raw = getResponse.body().string();

    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    Certificate cert = cf.generateCertificate(new ByteArrayInputStream(raw.getBytes(UTF_8)));
    return Optional.of((X509Certificate) cert);
  }


  private void initFirstNode(CouchbaseContainer container, String seedHost, int seedPort) throws Exception {
    httpClient.newCall(new Request.Builder()
      .header("Authorization", Credentials.basic(adminUsername, adminPassword))
      .url("http://" + seedHost + ":" + seedPort + "/pools/default")
      .post(new FormBody.Builder()
        .add("memoryQuota", "256")
        .add("indexMemoryQuota", "256")
        .build())
      .build())
      .execute();

    httpClient.newCall(new Request.Builder()
      .header("Authorization", Credentials.basic(adminUsername, adminPassword))
      .url("http://" + seedHost + ":" + seedPort + "/node/controller/setupServices")
      .post(new FormBody.Builder()
        .add("services", "kv,n1ql,index,fts")
        .build())
      .build())
      .execute();

    httpClient.newCall(new Request.Builder()
      .header("Authorization", Credentials.basic(adminUsername, adminPassword))
      .url("http://" + seedHost + ":" + seedPort + "/settings/web")
      .post(new FormBody.Builder()
        .add("username", adminUsername)
        .add("password", adminPassword)
        .add("port", "" + seedPort)
        .build())
      .build())
      .execute();

    createNodeWaitStrategy().waitUntilReady(container);
  }

  private HttpWaitStrategy createNodeWaitStrategy() {
    return new HttpWaitStrategy()
      .forPath("/pools/default/")
      .withBasicCredentials(adminUsername, adminPassword)
      .forStatusCode(HTTP_OK)
      .forResponsePredicate(response -> {
          try {
            return Optional.of(
              MAPPER.readTree(response.getBytes(UTF_8)))
                .map(n -> n.at("/nodes/0/status"))
                .map(JsonNode::asText)
                .map("healthy"::equals)
                .orElse(false);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      );
  }

  @Override
  public void close() {
    LOGGER.info("Stopping Containerized Cluster of {} nodes", containers.size());
    long start = System.nanoTime();
    for (CouchbaseContainer container : containers) {
      container.stop();
    }
    long end = System.nanoTime();
    LOGGER.debug("Stopping Containerized Cluster took {}s", TimeUnit.NANOSECONDS.toSeconds(end - start));
  }
}
