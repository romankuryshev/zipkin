package zipkin2.storage.clickhouse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.testcontainers.junit.jupiter.Testcontainers;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@Tag("docker")
class ClickHouseClusterTest {

  @Test
  void twoNodes_allSpansReachAtLeastOneNode() throws Exception {
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

      List<List<Span>> fromNode1 = node1.newStorageBuilder().build().spanStore().getTraces(request).execute();
      List<List<Span>> fromNode2 = node2.newStorageBuilder().build().spanStore().getTraces(request).execute();

      int total = fromNode1.size() + fromNode2.size();
      assertThat(total).isEqualTo(10);
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
