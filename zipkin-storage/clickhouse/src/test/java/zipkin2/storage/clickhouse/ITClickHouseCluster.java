package zipkin2.storage.clickhouse;

import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import zipkin2.Endpoint;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@Tag("docker")
class ITClickHouseCluster {

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
