package zipkin2.storage.clickhouse;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
class ClickHouseClusterTest {

  static final int HTTP_PORT = 8123;
  static final String IMAGE = "clickhouse/clickhouse-server:24.3-alpine";

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

  @Test
  void clusterMode_allSpansVisibleFromSingleEndpoint() throws Exception {
    Network network = Network.newNetwork();

    try (
      GenericContainer<?> node1 = clusterNode(network, 1, "clickhouse-1");
      GenericContainer<?> node2 = clusterNode(network, 2, "clickhouse-2");
      GenericContainer<?> node3 = clusterNode(network, 3, "clickhouse-3")
    ) {
      node1.start();
      node2.start();
      node3.start();

      ClickHouseStorage storage = new ClickHouseStorage.Builder()
        .setHost(node1.getHost())
        .setPort(node1.getMappedPort(HTTP_PORT))
        .setClusterName("zipkin_cluster")
        .setDatabase("zipkin")
        .setUsername("default")
        .setPassword("")
        .setEnsureSchema(true)
        .setIncludeSpanStatistics(false)
        .build();

      long now = System.currentTimeMillis();
      for (int i = 1; i <= 10; i++) {
        storage.spanConsumer().accept(List.of(
          Span.newBuilder()
            .traceId(String.format("%016d", i))
            .id("1")
            .name("test-op")
            .timestamp((now - i * 100L) * 1000L)
            .duration(1000L)
            .localEndpoint(Endpoint.newBuilder().serviceName("svc").build())
            .build()
        )).execute();
      }
      storage.forceFlush();

      QueryRequest request = QueryRequest.newBuilder()
        .endTs(now + 60_000)
        .lookback(TimeUnit.DAYS.toMillis(1))
        .limit(100)
        .build();

      List<List<Span>> all = storage.spanStore().getTraces(request).execute();
      assertThat(all).hasSize(10);

      storage.close();
    }
  }

  @Test
  void allNodesDown_flushThrowsException() throws Exception {
    try (ClickHouseContainer node1 = new ClickHouseContainer();
         ClickHouseContainer node2 = new ClickHouseContainer()) {
      node1.start();
      node2.start();

      ClickHouseStorage storage = new ClickHouseStorage.Builder()
        .addClusterNode(node1.getHost(), node1.getMappedPort(ClickHouseContainer.PORT))
        .addClusterNode(node2.getHost(), node2.getMappedPort(ClickHouseContainer.PORT))
        .setDatabase("zipkin")
        .setUsername("default")
        .setPassword("")
        .setEnsureSchema(false)
        .setIncludeSpanStatistics(false)
        .build();

      node1.stop();
      node2.stop();

      long now = System.currentTimeMillis();
      storage.spanConsumer().accept(List.of(
        Span.newBuilder()
          .traceId("000000000000001a")
          .id("1")
          .name("test")
          .timestamp(now * 1000L)
          .duration(1000L)
          .localEndpoint(Endpoint.newBuilder().serviceName("svc").build())
          .build()
      )).execute();

      assertThatThrownBy(() -> storage.forceFlush())
        .isInstanceOf(RuntimeException.class);

      storage.close();
    }
  }
}
