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

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import org.apache.commons.compress.utils.Sets;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.SocatContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.ThrowingFunction;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CouchbaseContainer extends GenericContainer<CouchbaseContainer> {

  public static final String STATIC_CONFIG = "/opt/couchbase/etc/couchbase/static_config";
  public static final String CAPI_CONFIG = "/opt/couchbase/etc/couchdb/default.d/capi.ini";

  // Testcontainers uses visibleassertions and it dumps annoying stuff in the logs. This
  // is the only way to silence it.
  static {
    System.setProperty("visibleassertions.silence", "true");
  }

  private final SupportedVersion version;

  private SocatContainer proxy;

  public CouchbaseContainer(final SupportedVersion version) {
    super("couchbase/server:" + version.containerVersion());
    this.version = version;

    withNetwork(Network.SHARED);
    withNetworkAliases("couchbase-" + Base58.randomString(6));
    setWaitStrategy(new HttpWaitStrategy().forPath("/ui/index.html"));
  }

  public SupportedVersion supportedVersion() {
    return version;
  }

  @Override
  public Set<Integer> getLivenessCheckPortNumbers() {
    return Sets.newHashSet(getMappedPort(CouchbasePort.REST));
  }

  @Override
  protected void doStart() {
    String networkAlias = getNetworkAliases().get(0);
    startProxy(networkAlias);

    for (CouchbasePort port : CouchbasePort.values()) {
      exposePortThroughProxy(networkAlias, port.originalPort(), getMappedPort(port));
    }
    super.doStart();
  }

  private void exposePortThroughProxy(String networkAlias, int originalPort, int mappedPort) {
    ExecCreateCmdResponse createCmdResponse = dockerClient
      .execCreateCmd(proxy.getContainerId())
      .withCmd("/usr/bin/socat", "TCP-LISTEN:" + originalPort + ",fork,reuseaddr", "TCP:" + networkAlias + ":" + mappedPort)
      .exec();

    dockerClient.execStartCmd(createCmdResponse.getId())
      .exec(new ExecStartResultCallback());
  }

  private void startProxy(String networkAlias) {
    proxy = new SocatContainer().withNetwork(getNetwork());

    for (CouchbasePort port : CouchbasePort.values()) {
      if (port.dynamic()) {
        proxy.withTarget(port.originalPort(), networkAlias);
      } else {
        proxy.addExposedPort(port.originalPort());
      }
    }
    proxy.setWaitStrategy(null);
    proxy.start();
  }

  @Override
  public List<Integer> getExposedPorts() {
    return proxy.getExposedPorts();
  }

  @Override
  public String getContainerIpAddress() {
    return proxy.getContainerIpAddress();
  }

  @Override
  public Integer getMappedPort(int originalPort) {
    return proxy.getMappedPort(originalPort);
  }

  public Integer getMappedPort(CouchbasePort port) {
    return getMappedPort(port.originalPort());
  }

  @Override
  public List<Integer> getBoundPortNumbers() {
    return proxy.getBoundPortNumbers();
  }

  @Override
  public void stop() {
    Stream.<Runnable>of(super::stop, proxy::stop).parallel().forEach(Runnable::run);
  }

  @Override
  protected void containerIsCreated(String containerId) {
    patchConfig(STATIC_CONFIG, this::addMappedPorts);
    // capi needs a special configuration, see https://developer.couchbase.com/documentation/server/current/install/install-ports.html
    patchConfig(CAPI_CONFIG, this::replaceCapiPort);
  }

  private void patchConfig(String configLocation, ThrowingFunction<String, String> patchFunction) {
    String patchedConfig = copyFileFromContainer(configLocation,
      inputStream -> patchFunction.apply(IOUtils.toString(inputStream, StandardCharsets.UTF_8)));
    copyFileToContainer(Transferable.of(patchedConfig.getBytes(StandardCharsets.UTF_8)), configLocation);
  }

  private String addMappedPorts(String originalConfig) {
    String portConfig = Stream.of(CouchbasePort.values())
      .filter(port -> !port.dynamic())
      .map(port -> String.format("{%s, %d}.", port.managerIdentifier(), getMappedPort(port)))
      .collect(Collectors.joining("\n"));
    return String.format("%s\n%s", originalConfig, portConfig);
  }

  private String replaceCapiPort(String originalConfig) {
    return Arrays.stream(originalConfig.split("\n"))
      .map(s -> (s.matches("port\\s*=\\s*" + CouchbasePort.CAPI.originalPort())) ? "port = " + getMappedPort(CouchbasePort.CAPI) : s)
      .collect(Collectors.joining("\n"));
  }

  enum CouchbasePort {
    REST("rest_port", 8091, true),
    CAPI("capi_port", 8092, false),
    QUERY("query_port", 8093, false),
    FTS("fts_http_port", 8094, false),
    CBAS("cbas_http_port", 8095, false),
    EVENTING("eventing_http_port", 8096, false),
    MEMCACHED_SSL("memcached_ssl_port", 11207, false),
    MEMCACHED("memcached_port", 11210, false),
    REST_SSL("ssl_rest_port", 18091, true),
    CAPI_SSL("ssl_capi_port", 18092, false),
    QUERY_SSL("ssl_query_port", 18093, false),
    FTS_SSL("fts_ssl_port", 18094, false),
    CBAS_SSL("cbas_ssl_port", 18095, false),
    EVENTING_SSL("eventing_ssl_port", 18096, false);

    final String managerIdentifier;

    final int originalPort;

    final boolean dynamic;

    CouchbasePort(String name, int originalPort, boolean dynamic) {
      this.managerIdentifier = name;
      this.originalPort = originalPort;
      this.dynamic = dynamic;
    }

    public String managerIdentifier() {
      return managerIdentifier;
    }

    public int originalPort() {
      return originalPort;
    }

    public boolean dynamic() {
      return dynamic;
    }
  }

}
