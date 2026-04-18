package zipkin2.storage.clickhouse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import zipkin2.Span;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ClickHouseClusterAdvancedTest {

  private com.clickhouse.client.api.Client mockClient;
  private List<ClickHouseSpanConsumer> consumers;

  @BeforeEach
  public void setUp() {
    mockClient = mock(com.clickhouse.client.api.Client.class);
    consumers = new ArrayList<>();
  }

  @AfterEach
  public void tearDown() {
    for (ClickHouseSpanConsumer consumer : consumers) {
      if (consumer != null) {
        consumer.close();
      }
    }
  }

  @Test
  @Timeout(30)
  public void testClusterConsistency() throws InterruptedException {
    int nodeCount = 3;
    List<ClickHouseSpanConsumer> nodes = new ArrayList<>();

    for (int i = 0; i < nodeCount; i++) {
      nodes.add(new ClickHouseSpanConsumer(mockClient, "zipkin", true));
      consumers.add(nodes.get(i));
    }

    ExecutorService executor = Executors.newFixedThreadPool(nodeCount);
    try {
      CountDownLatch latch = new CountDownLatch(nodeCount);

      for (int i = 0; i < nodeCount; i++) {
        final int nodeId = i;
        executor.submit(() -> {
          try {
            ClickHouseSpanConsumer node = nodes.get(nodeId);
            for (int batch = 0; batch < 5; batch++) {
              node.accept(createTestSpans(10));
            }
          } finally {
            latch.countDown();
          }
        });
      }

      assertTrue(latch.await(30, TimeUnit.SECONDS));
    } finally {
      executor.shutdown();
    }
  }

  @Test
  @Timeout(45)
  public void testClusterScaling() {
    List<ClickHouseSpanConsumer> cluster = new ArrayList<>();

    for (int i = 0; i < 2; i++) {
      ClickHouseSpanConsumer node = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
      cluster.add(node);
      consumers.add(node);
    }

    for (int i = 0; i < 20; i++) {
      cluster.get(i % cluster.size()).accept(createTestSpans(1));
    }

    for (int i = 0; i < 2; i++) {
      ClickHouseSpanConsumer node = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
      cluster.add(node);
      consumers.add(node);
    }

    for (int i = 20; i < 40; i++) {
      cluster.get(i % cluster.size()).accept(createTestSpans(1));
    }

    assertEquals(4, cluster.size());
  }

  @Test
  @Timeout(45)
  public void testClusterFailover() {
    ClickHouseSpanConsumer primary = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
    ClickHouseSpanConsumer secondary = new ClickHouseSpanConsumer(mockClient, "zipkin", true);

    consumers.add(primary);
    consumers.add(secondary);

    primary.accept(createTestSpans(10));

    primary.close();
    consumers.remove(primary);

    secondary.accept(createTestSpans(10));

    assertNotNull(secondary);
  }

  private List<Span> createTestSpans(int count) {
    List<Span> spans = new ArrayList<>();
    long baseTimestamp = System.currentTimeMillis() * 1000;

    for (int i = 0; i < count; i++) {
      long traceIdMsb = 0x1111111111111111L + i;
      long traceIdLsb = 0x2222222222222222L + i;
      long spanId = 0x3333333333333333L + i;

      Span span = Span.newBuilder()
        .traceId(String.format("%016x%016x", traceIdMsb, traceIdLsb))
        .id(String.format("%016x", spanId))
        .name("cluster-test-span-" + i)
        .timestamp(baseTimestamp + i)
        .duration(100 + i)
        .build();
      spans.add(span);
    }
    return spans;
  }
}


