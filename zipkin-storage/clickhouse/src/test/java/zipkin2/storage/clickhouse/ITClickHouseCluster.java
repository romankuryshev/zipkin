package zipkin2.storage.clickhouse;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.utility.DockerImageName.parse;

@Testcontainers
@Tag("docker")
class ITClickHouseCluster {

  static final int HTTP_PORT = 8123;
  static final String IMAGE = "clickhouse/clickhouse-server:24.3-alpine";
  static final String CLUSTER = "zipkin_cluster";

  static Network network;
  static GenericContainer<?> node1;
  static GenericContainer<?> node2;
  static GenericContainer<?> node3;
  static ClickHouseStorage storage;

  static GenericContainer<?> clusterNode(Network network, int shard, String alias) {
    GenericContainer<?> container = new GenericContainer<>(parse(IMAGE))
      .withNetwork(network)
      .withNetworkAliases(alias)
      .withExposedPorts(HTTP_PORT)
      .withEnv("CLICKHOUSE_DB", "zipkin")
      .withEnv("CLICKHOUSE_USER", "default")
      .withEnv("CLICKHOUSE_PASSWORD", "")
      .withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
      .withCopyFileToContainer(
        MountableFile.forClasspathResource("cluster/cluster.xml"),
        "/etc/clickhouse-server/config.d/cluster.xml"
      )
      .withCopyFileToContainer(
        MountableFile.forClasspathResource("cluster/macros-shard" + shard + ".xml"),
        "/etc/clickhouse-server/config.d/macros.xml"
      )
      .waitingFor(Wait.forHttp("/ping").forPort(HTTP_PORT).withStartupTimeout(Duration.ofMinutes(2)));

    if (shard == 1) {
      container.withCopyFileToContainer(
        MountableFile.forClasspathResource("cluster/keeper.xml"),
        "/etc/clickhouse-server/config.d/keeper.xml"
      );
    }

    return container;
  }

  @BeforeAll static void startCluster() {
    network = Network.newNetwork();
    node1 = clusterNode(network, 1, "clickhouse-1");
    node2 = clusterNode(network, 2, "clickhouse-2");
    node3 = clusterNode(network, 3, "clickhouse-3");
    node1.start();
    node2.start();
    node3.start();

    storage = new ClickHouseStorage.Builder()
      .setHost(node1.getHost())
      .setPort(node1.getMappedPort(HTTP_PORT))
      .setClusterName(CLUSTER)
      .setDatabase("zipkin")
      .setUsername("default")
      .setPassword("")
      .setEnsureSchema(true)
      .setIncludeSpanStatistics(false)
      .build();
  }

  @AfterAll static void stopCluster() {
    if (storage != null) storage.close();
    if (node1 != null) node1.stop();
    if (node2 != null) node2.stop();
    if (node3 != null) node3.stop();
    if (network != null) network.close();
  }

  @BeforeEach void clear() throws Exception {
    storage.getClient()
      .execute("TRUNCATE TABLE IF EXISTS zipkin.spans_local ON CLUSTER '" + CLUSTER + "'")
      .get();
  }

  /** Distributed inserts are forwarded to shards asynchronously; force the flush so a read
   *  immediately after a write sees every shard's data. */
  private void writeAndMakeVisible(List<Span> spans) throws Exception {
    storage.spanConsumer().accept(spans).execute();
    storage.forceFlush();
    storage.getClient().execute("SYSTEM FLUSH DISTRIBUTED zipkin.spans").get();
  }

  @Test
  void writeReadBackSingleTrace() throws Exception {
    Span span = Span.newBuilder()
      .traceId("0000000000000001")
      .id("000000000000000a")
      .name("get /one")
      .timestamp(System.currentTimeMillis() * 1000L)
      .duration(1234L)
      .localEndpoint(Endpoint.newBuilder().serviceName("svc-one").ip("127.0.0.1").port(8080).build())
      .build();

    writeAndMakeVisible(List.of(span));

    List<Span> trace = storage.spanStore().getTrace("0000000000000001").execute();

    assertThat(trace).hasSize(1);
    assertThat(trace.get(0).name()).isEqualTo("get /one");
    assertThat(trace.get(0).localServiceName()).isEqualTo("svc-one");
    assertThat(trace.get(0).localEndpoint().ipv4()).isEqualTo("127.0.0.1");
  }

  @Test
  void writeReadBackMultipleTracesAcrossShards() throws Exception {
    long now = System.currentTimeMillis();
    for (int i = 1; i <= 6; i++) {
      writeAndMakeVisible(List.of(
        Span.newBuilder()
          .traceId(String.format("%016d", i))
          .id("000000000000000a")
          .name("op")
          .timestamp((now - i) * 1000L)
          .duration(1000L)
          .localEndpoint(Endpoint.newBuilder().serviceName("svc-many").build())
          .build()
      ));
    }

    QueryRequest request = QueryRequest.newBuilder()
      .serviceName("svc-many")
      .endTs(now + 60_000)
      .lookback(TimeUnit.DAYS.toMillis(1))
      .limit(100)
      .build();

    List<List<Span>> traces = storage.spanStore().getTraces(request).execute();

    assertThat(traces).hasSize(6);
  }

  @Test
  void allNodesDown_flushThrowsException() throws Exception {
    try (ClickHouseContainer down1 = new ClickHouseContainer();
         ClickHouseContainer down2 = new ClickHouseContainer()) {
      down1.start();
      down2.start();

      ClickHouseStorage downStorage = new ClickHouseStorage.Builder()
        .addClusterNode(down1.getHost(), down1.getMappedPort(ClickHouseContainer.PORT))
        .addClusterNode(down2.getHost(), down2.getMappedPort(ClickHouseContainer.PORT))
        .setDatabase("zipkin")
        .setUsername("default")
        .setPassword("")
        .setEnsureSchema(false)
        .setIncludeSpanStatistics(false)
        .build();

      down1.stop();
      down2.stop();

      long now = System.currentTimeMillis();
      downStorage.spanConsumer().accept(List.of(
        Span.newBuilder()
          .traceId("000000000000001a")
          .id("1")
          .name("test")
          .timestamp(now * 1000L)
          .duration(1000L)
          .localEndpoint(Endpoint.newBuilder().serviceName("svc").build())
          .build()
      )).execute();

      assertThatThrownBy(() -> downStorage.forceFlush())
        .isInstanceOf(RuntimeException.class);

      downStorage.close();
    }
  }
}
