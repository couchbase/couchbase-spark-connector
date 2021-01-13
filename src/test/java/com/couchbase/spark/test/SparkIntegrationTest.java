package com.couchbase.spark.test;

import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.scala.Cluster;
import com.couchbase.spark.core.CouchbaseConfig;
import com.couchbase.spark.core.CouchbaseConnection;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SparkIntegrationTest extends ClusterAwareIntegrationTest {

  private static SparkSession sparkSession;

  @BeforeAll
  static void beforeAll() {
    sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("Couchbase Spark Test Suite")
      .config("spark.couchbase.connectionString", connectionString())
      .config("spark.couchbase.username", config().adminUsername())
      .config("spark.couchbase.password", config().adminPassword())
      .config("spark.couchbase.implicitBucket", config().bucketname())
      .getOrCreate();
  }

  @AfterAll
  static void afterAll() {
    sparkSession.close();
  }

  protected SparkSession sparkSession() {
    return sparkSession;
  }

  protected String bucketName() {
    return config().bucketname();
  }

  protected CouchbaseConnection connection() {
    return CouchbaseConnection.apply();
  }

  protected CouchbaseConfig couchbaseConfig() {
    return CouchbaseConfig.apply(sparkSession.sparkContext().conf());
  }

  protected Cluster cluster() {
    return connection().cluster(couchbaseConfig());
  }

  protected static String connectionString() {
    return seedNodes().stream().map(s -> {
      if (s.kvPort().isPresent()) {
        return s.address() + ":" + s.kvPort().get();
      } else {
        return s.address();
      }
    }).collect(Collectors.joining(","));
  }

  protected static Set<SeedNode> seedNodes() {
    return config().nodes().stream().map(cfg -> SeedNode.create(
      cfg.hostname(),
      Optional.ofNullable(cfg.ports().get(Services.KV)),
      Optional.ofNullable(cfg.ports().get(Services.MANAGER))
    )).collect(Collectors.toSet());
  }

}
