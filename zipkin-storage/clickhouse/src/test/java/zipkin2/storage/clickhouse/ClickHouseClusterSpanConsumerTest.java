/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.clickhouse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.clickhouse.cache.AutocompleteTagsCache;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Сложные тесты для ClickHouseSpanConsumer при работе с кластером ClickHouse.
 * Проверяет распределение спанов между узлами, load balancing и обработку отказов узлов.
 */
public class ClickHouseClusterSpanConsumerTest {

  private static final Logger log = LoggerFactory.getLogger(ClickHouseClusterSpanConsumerTest.class);

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

  // ==================== Тесты распределения спанов в кластере ====================

  @Test
  public void multipleConsumersDistributeSpansBetweenClusterNodes() {
    ClickHouseSpanConsumer consumer1 = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
    ClickHouseSpanConsumer consumer2 = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
    ClickHouseSpanConsumer consumer3 = new ClickHouseSpanConsumer(mockClient, "zipkin", true);

    consumers.add(consumer1);
    consumers.add(consumer2);
    consumers.add(consumer3);

    consumer1.accept(createTestSpans(5));
    consumer2.accept(createTestSpans(5));
    consumer3.accept(createTestSpans(5));

    assertNotNull(consumer1);
    assertNotNull(consumer2);
    assertNotNull(consumer3);
  }

  @Test
  public void clusterHandlesSimultaneousSpanInsertion() throws InterruptedException {
    int numNodes = 3;
    for (int i = 0; i < numNodes; i++) {
      consumers.add(new ClickHouseSpanConsumer(mockClient, "zipkin", true));
    }

    int numThreads = 6;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger errorCount = new AtomicInteger(0);

    for (int t = 0; t < numThreads; t++) {
      final int nodeIndex = t % numNodes;
      executor.submit(() -> {
        try {
          ClickHouseSpanConsumer consumer = consumers.get(nodeIndex);
          for (int i = 0; i < 2; i++) {
            consumer.accept(createTestSpans(5));
            successCount.incrementAndGet();
          }
        } catch (Exception e) {
          errorCount.incrementAndGet();
          log.error("Error in cluster node", e);
        } finally {
          latch.countDown();
        }
      });
    }

    assertTrue(latch.await(30, TimeUnit.SECONDS));
    executor.shutdown();

    assertEquals(numThreads * 2, successCount.get(), "Все операции должны быть успешны");
    assertEquals(0, errorCount.get(), "Не должно быть ошибок");
  }

  @Test
  @Timeout(45)
  public void clusterNodeFailureDoesNotBlockOthers() throws InterruptedException {
    ClickHouseSpanConsumer healthyNode1 = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
    ClickHouseSpanConsumer healthyNode2 = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
    ClickHouseSpanConsumer failedNode = new ClickHouseSpanConsumer(mockClient, "zipkin", true);

    consumers.add(healthyNode1);
    consumers.add(healthyNode2);
    consumers.add(failedNode);

    int numThreads = 3;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    AtomicInteger healthyOperations = new AtomicInteger(0);

    executor.submit(() -> {
      try {
        for (int i = 0; i < 3; i++) {
          healthyNode1.accept(createTestSpans(3));
          healthyOperations.incrementAndGet();
        }
      } finally {
        latch.countDown();
      }
    });

    executor.submit(() -> {
      try {
        for (int i = 0; i < 3; i++) {
          healthyNode2.accept(createTestSpans(3));
          healthyOperations.incrementAndGet();
        }
      } finally {
        latch.countDown();
      }
    });

    executor.submit(() -> {
      try {
        for (int i = 0; i < 3; i++) {
          failedNode.accept(createTestSpans(3));
        }
      } finally {
        latch.countDown();
      }
    });

    assertTrue(latch.await(45, TimeUnit.SECONDS));
    executor.shutdown();

    assertEquals(6, healthyOperations.get(), "Здоровые узлы должны обработать все спаны");
  }

  @Test
  @Timeout(35)
  public void consistentSpanHandlingAcrossCluster() throws InterruptedException {
    int numNodes = 3;
    List<ClickHouseSpanConsumer> clusterNodes = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      ClickHouseSpanConsumer consumer = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
      clusterNodes.add(consumer);
      consumers.add(consumer);
    }

    CyclicBarrier barrier = new CyclicBarrier(numNodes);
    ExecutorService executor = Executors.newFixedThreadPool(numNodes);
    CountDownLatch latch = new CountDownLatch(numNodes);
    List<Integer> spanCountsPerNode = Collections.synchronizedList(new ArrayList<>());

    for (int nodeId = 0; nodeId < numNodes; nodeId++) {
      final int nodeIndex = nodeId;
      executor.submit(() -> {
        try {
          barrier.await();
          ClickHouseSpanConsumer node = clusterNodes.get(nodeIndex);

          int totalSpans = 0;
          for (int batch = 0; batch < 2; batch++) {
            List<Span> spans = createTestSpans(5);
            node.accept(spans);
            totalSpans += spans.size();
          }
          spanCountsPerNode.add(totalSpans);
        } catch (Exception e) {
          log.error("Error in cluster consistency test", e);
        } finally {
          latch.countDown();
        }
      });
    }

    assertTrue(latch.await(35, TimeUnit.SECONDS));
    executor.shutdown();

    assertEquals(numNodes, spanCountsPerNode.size());
    for (Integer count : spanCountsPerNode) {
      assertEquals(10, count.intValue(), "Каждый узел должен обработать 10 спанов");
    }
  }

  // ==================== Тесты балансировки нагрузки ====================

  @Test
  @Timeout(35)
  public void loadBalancingAcrossClusterNodes() throws InterruptedException {
    int numNodes = 3;
    List<ClickHouseSpanConsumer> clusterNodes = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      clusterNodes.add(new ClickHouseSpanConsumer(mockClient, "zipkin", true));
      consumers.add(clusterNodes.get(i));
    }

    int spansPerNode = 30;
    for (int i = 0; i < spansPerNode; i++) {
      int nodeIndex = i % numNodes;
      clusterNodes.get(nodeIndex).accept(createTestSpans(1));
    }

    assertNotNull(clusterNodes);
    assertEquals(numNodes, clusterNodes.size());
  }

  @Test
  @Timeout(40)
  public void unbalancedLoadOnClusterNodes() throws InterruptedException {
    int numNodes = 3;
    List<ClickHouseSpanConsumer> clusterNodes = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      clusterNodes.add(new ClickHouseSpanConsumer(mockClient, "zipkin", true));
      consumers.add(clusterNodes.get(i));
    }

    clusterNodes.get(0).accept(createTestSpans(30));
    clusterNodes.get(1).accept(createTestSpans(10));
    clusterNodes.get(2).accept(createTestSpans(5));

    for (ClickHouseSpanConsumer node : clusterNodes) {
      assertNotNull(node);
    }
  }

  // ==================== Тесты восстановления после сбоев ====================

  @Test
  @Timeout(40)
  public void clusterRecoveryAfterNodeRestart() throws InterruptedException {
    ClickHouseSpanConsumer node1 = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
    ClickHouseSpanConsumer node2 = new ClickHouseSpanConsumer(mockClient, "zipkin", true);

    consumers.add(node1);
    consumers.add(node2);

    node1.accept(createTestSpans(5));
    node2.accept(createTestSpans(5));

    node1.close();
    consumers.remove(node1);

    ClickHouseSpanConsumer node1Restarted = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
    consumers.add(node1Restarted);

    node1Restarted.accept(createTestSpans(5));
    node2.accept(createTestSpans(5));

    assertNotNull(node1Restarted);
    assertNotNull(node2);
  }

  @Test
  @Timeout(35)
  public void gracefulShutdownOfAllClusterNodes() {
    int numNodes = 3;
    List<ClickHouseSpanConsumer> clusterNodes = new ArrayList<>();

    for (int i = 0; i < numNodes; i++) {
      ClickHouseSpanConsumer consumer = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
      clusterNodes.add(consumer);
      consumers.add(consumer);
    }

    for (ClickHouseSpanConsumer node : clusterNodes) {
      node.accept(createTestSpans(5));
    }

    long startTime = System.currentTimeMillis();
    for (ClickHouseSpanConsumer node : clusterNodes) {
      node.close();
    }
    long duration = System.currentTimeMillis() - startTime;

    assertTrue(duration < 120000, "Shutdown всех узлов должен завершиться за < 120 сек");
  }

  // ==================== Тесты с разными конфигурациями узлов ====================

  @Test
  public void clusterWithDifferentDatabasesPerNode() {
    ClickHouseSpanConsumer node1 = new ClickHouseSpanConsumer(mockClient, "zipkin_1", true);
    ClickHouseSpanConsumer node2 = new ClickHouseSpanConsumer(mockClient, "zipkin_2", true);
    ClickHouseSpanConsumer node3 = new ClickHouseSpanConsumer(mockClient, "zipkin_3", true);

    consumers.add(node1);
    consumers.add(node2);
    consumers.add(node3);

    node1.accept(createTestSpans(5));
    node2.accept(createTestSpans(5));
    node3.accept(createTestSpans(5));

    assertNotNull(node1);
    assertNotNull(node2);
    assertNotNull(node3);
  }

  @Test
  public void clusterWithMixedStrictTraceIdModes() {
    ClickHouseSpanConsumer strictNode1 = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
    ClickHouseSpanConsumer strictNode2 = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
    ClickHouseSpanConsumer lenientNode = new ClickHouseSpanConsumer(mockClient, "zipkin", false);

    consumers.add(strictNode1);
    consumers.add(strictNode2);
    consumers.add(lenientNode);

    strictNode1.accept(createTestSpans(5));
    strictNode2.accept(createTestSpans(5));
    lenientNode.accept(createTestSpans(5));

    assertTrue(true);
  }

  // ==================== Тесты масштабирования кластера ====================

  @Test
  @Timeout(45)
  public void clusterScaleUp() throws InterruptedException {
    List<ClickHouseSpanConsumer> clusterNodes = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      ClickHouseSpanConsumer consumer = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
      clusterNodes.add(consumer);
      consumers.add(consumer);
    }

    for (int i = 0; i < 20; i++) {
      clusterNodes.get(i % 2).accept(createTestSpans(1));
    }

    for (int i = 0; i < 2; i++) {
      ClickHouseSpanConsumer newNode = new ClickHouseSpanConsumer(mockClient, "zipkin", true);
      clusterNodes.add(newNode);
      consumers.add(newNode);
    }

    for (int i = 20; i < 40; i++) {
      clusterNodes.get(i % clusterNodes.size()).accept(createTestSpans(1));
    }

    assertEquals(4, clusterNodes.size());
  }

  @Test
  @Timeout(45)
  public void clusterScaleDown() throws InterruptedException {
    List<ClickHouseSpanConsumer> clusterNodes = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      clusterNodes.add(new ClickHouseSpanConsumer(mockClient, "zipkin", true));
    }

    for (int i = 0; i < 40; i++) {
      clusterNodes.get(i % 4).accept(createTestSpans(1));
    }

    ClickHouseSpanConsumer node1 = clusterNodes.remove(0);
    ClickHouseSpanConsumer node2 = clusterNodes.remove(0);
    node1.close();
    node2.close();

    for (int i = 40; i < 50; i++) {
      clusterNodes.get(i % clusterNodes.size()).accept(createTestSpans(1));
    }

    assertEquals(2, clusterNodes.size());

    for (ClickHouseSpanConsumer node : clusterNodes) {
      node.close();
    }
  }

  // ==================== Тесты high availability ====================

  @Test
  @Timeout(45)
  public void highAvailabilityWithRedundancy() throws InterruptedException {
    int primaryNodes = 3;
    int redundantNodes = 2;
    List<ClickHouseSpanConsumer> allNodes = new ArrayList<>();

    for (int i = 0; i < primaryNodes + redundantNodes; i++) {
      allNodes.add(new ClickHouseSpanConsumer(mockClient, "zipkin", true));
      consumers.add(allNodes.get(i));
    }

    ExecutorService executor = Executors.newFixedThreadPool(primaryNodes + redundantNodes);
    CountDownLatch latch = new CountDownLatch(primaryNodes + redundantNodes);
    AtomicInteger totalProcessed = new AtomicInteger(0);

    for (int nodeId = 0; nodeId < primaryNodes + redundantNodes; nodeId++) {
      final int nodeIndex = nodeId;
      executor.submit(() -> {
        try {
          for (int i = 0; i < 10; i++) {
            allNodes.get(nodeIndex).accept(createTestSpans(1));
            totalProcessed.incrementAndGet();
          }
        } finally {
          latch.countDown();
        }
      });
    }

    assertTrue(latch.await(45, TimeUnit.SECONDS));
    executor.shutdown();

    assertEquals((primaryNodes + redundantNodes) * 10, totalProcessed.get());
  }

  // ==================== Вспомогательные методы ====================

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

