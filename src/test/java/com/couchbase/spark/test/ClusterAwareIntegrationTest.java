/*
 * Copyright (c) 2019 Couchbase, Inc.
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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Parent class which drives all dynamic integration tests based on the configured
 * cluster setup.
 *
 * @since 2.0.0
 */
@ExtendWith(ClusterInvocationProvider.class)
public class ClusterAwareIntegrationTest {

  private static TestClusterConfig testClusterConfig;

  @BeforeAll
  static void setup(TestClusterConfig config) {
    testClusterConfig = config;
  }

  /**
   * Returns the current config for the integration test cluster.
   */
  public static TestClusterConfig config() {
    return testClusterConfig;
  }

}
