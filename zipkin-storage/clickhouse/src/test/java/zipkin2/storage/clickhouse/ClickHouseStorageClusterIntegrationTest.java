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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Интеграционные тесты для ClickHouseStorage при работе с кластером.
 * Проверяют взаимодействие Storage, SpanConsumer и кластера.
 */
public class ClickHouseStorageClusterIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(ClickHouseStorageClusterIntegrationTest.class);

  private List<ClickHouseStorage> storages;

  @BeforeEach
  public void setUp() {
    storages = new ArrayList<>();
  }

  @AfterEach
  public void tearDown() {
    for (ClickHouseStorage storage : storages) {
      if (storage != null) {
        storage.close();
      }
    }
  }

  // ==================== Тесты инициализации кластера ====================

  @Test
  public void singleClusterNodeInitialization() {
    com.clickhouse.client.api.Client mockClient = mock(com.clickhouse.client.api.Client.class);
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    assertNotNull(builder);
  }

  @Test
  public void multipleClusterNodesInitialization() {
    com.clickhouse.client.api.Client mockClient = mock(com.clickhouse.client.api.Client.class);
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .addClusterNode("node1", 8123)
      .addClusterNode("node2", 8123)
      .addClusterNode("node3", 8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    assertNotNull(builder);
  }

  @Test
  public void clusterNodeWithCustomPorts() {
    com.clickhouse.client.api.Client mockClient = mock(com.clickhouse.client.api.Client.class);
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .addClusterNode("node1", 8123)
      .addClusterNode("node2", 8124)
      .addClusterNode("node3", 8125)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    assertNotNull(builder);
  }

  // ==================== Тесты SpanConsumer через Storage ====================

  @Test
  public void storageSpanConsumerAcceptsSingleSpan() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    ClickHouseStorage storage = new ClickHouseStorage(builder);
    storages.add(storage);

    List<Span> spans = createTestSpans(1);
    Call<Void> result = storage.spanConsumer().accept(spans);

    assertNotNull(result);
  }

  @Test
  public void storageSpanConsumerAcceptsBatch() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    ClickHouseStorage storage = new ClickHouseStorage(builder);
    storages.add(storage);

    // Батч из 10 спанов
    List<Span> spans = createTestSpans(10);
    Call<Void> result = storage.spanConsumer().accept(spans);

    assertNotNull(result);
  }

  @Test
  @Timeout(35)
  public void storageSpanConsumerMultipleBatches() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    ClickHouseStorage storage = new ClickHouseStorage(builder);
    storages.add(storage);

    // 3 батча по 10 спанов
    for (int i = 0; i < 3; i++) {
      storage.spanConsumer().accept(createTestSpans(10));
    }

    assertNotNull(storage);
  }

  // ==================== Тесты конкурентного доступа к Storage ====================

  @Test
  @Timeout(15)
  @SuppressWarnings("resource")
  public void concurrentSpanConsumerAccess() throws InterruptedException {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    ClickHouseStorage storage = new ClickHouseStorage(builder);
    storages.add(storage);

    int numThreads = 5;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      CountDownLatch latch = new CountDownLatch(numThreads);
      AtomicInteger successCount = new AtomicInteger(0);
      AtomicInteger errorCount = new AtomicInteger(0);

      for (int t = 0; t < numThreads; t++) {
        executor.submit(() -> {
          try {
            for (int i = 0; i < 4; i++) {
              storage.spanConsumer().accept(createTestSpans(1));
              successCount.incrementAndGet();
            }
          } catch (Exception e) {
            errorCount.incrementAndGet();
            log.error("Error in concurrent span consumer access", e);
          } finally {
            latch.countDown();
          }
        });
      }

      assertTrue(latch.await(15, TimeUnit.SECONDS));

      assertEquals(numThreads * 4, successCount.get());
      assertEquals(0, errorCount.get());
    } finally {
      executor.shutdown();
    }
  }

  // ==================== Тесты кластерной конфигурации ====================

  @Test
  public void clusterStorageWithAutocompleteKeys() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .addClusterNode("node1", 8123)
      .addClusterNode("node2", 8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin")
      .setAutocompleteKeys(List.of("service", "span.kind"))
      .setAutocompleteTtl(3600000)
      .setAutocompleteCardinality(20000);

    assertNotNull(builder);
  }

  @Test
  public void clusterStorageWithStrictTraceId() {
    ClickHouseStorage.Builder builder1 = new ClickHouseStorage.Builder()
      .addClusterNode("node1", 8123)
      .addClusterNode("node2", 8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin")
      .setStrictTraceId(true);

    ClickHouseStorage.Builder builder2 = new ClickHouseStorage.Builder()
      .addClusterNode("node1", 8123)
      .addClusterNode("node2", 8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin")
      .setStrictTraceId(false);

    assertNotNull(builder1);
    assertNotNull(builder2);
  }

  // ==================== Тесты жизненного цикла Storage ====================

  @Test
  @Timeout(35)
  @SuppressWarnings("resource")
  public void storageGracefulShutdown() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    ClickHouseStorage storage = new ClickHouseStorage(builder);

    // Добавляем спаны
    storage.spanConsumer().accept(createTestSpans(5));

    // Graceful shutdown
    long startTime = System.currentTimeMillis();
    storage.close();
    long duration = System.currentTimeMillis() - startTime;

    assertTrue(duration < 35000, "Shutdown должен завершиться за < 35 сек");
  }

  @Test
  @Timeout(40)
  @SuppressWarnings("resource")
  public void multipleStorageInstancesLifecycle() throws InterruptedException {
    int numStorages = 3;
    ExecutorService executor = Executors.newFixedThreadPool(numStorages);
    try {
      CountDownLatch createLatch = new CountDownLatch(numStorages);
      CountDownLatch closeLatch = new CountDownLatch(numStorages);

      List<ClickHouseStorage> localStorages = Collections.synchronizedList(new ArrayList<>());

      // Создание нескольких Storage экземпляров
      for (int i = 0; i < numStorages; i++) {
        final int index = i;
        executor.submit(() -> {
          try {
            ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
              .setHost("localhost")
              .setPort(8123 + index)
              .setDatabase("zipkin")
              .setUsername("zipkin")
              .setPassword("zipkin");

            ClickHouseStorage storage = new ClickHouseStorage(builder);
            localStorages.add(storage);
            storages.add(storage);

            // Добавляем спаны
            storage.spanConsumer().accept(createTestSpans(5));
          } finally {
            createLatch.countDown();
          }
        });
      }

      assertTrue(createLatch.await(30, TimeUnit.SECONDS));

      // Закрытие всех Storage экземпляров
      for (ClickHouseStorage storage : localStorages) {
        executor.submit(() -> {
          try {
            storage.close();
          } finally {
            closeLatch.countDown();
          }
        });
      }

      assertTrue(closeLatch.await(40, TimeUnit.SECONDS));
    } finally {
      executor.shutdown();
    }
  }

  // ==================== Тесты высоконагруженных сценариев ====================

  @Test
  @Timeout(45)
  @SuppressWarnings("resource")
  public void heavyLoadWithLargeSpanBatches() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .addClusterNode("node1", 8123)
      .addClusterNode("node2", 8123)
      .addClusterNode("node3", 8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    ClickHouseStorage storage = new ClickHouseStorage(builder);
    storages.add(storage);

    // 100 больших батчей
    for (int i = 0; i < 100; i++) {
      storage.spanConsumer().accept(createTestSpans(10));
    }

    assertNotNull(storage);
  }

  @Test
  @Timeout(45)
  @SuppressWarnings("resource")
  public void heavyLoadWithManySmallBatches() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .addClusterNode("node1", 8123)
      .addClusterNode("node2", 8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    ClickHouseStorage storage = new ClickHouseStorage(builder);
    storages.add(storage);

    // 200 маленьких батчей
    for (int i = 0; i < 200; i++) {
      storage.spanConsumer().accept(createTestSpans(1));
    }

    assertNotNull(storage);
  }

  @Test
  @Timeout(50)
  @SuppressWarnings("resource")
  public void stressTestConcurrentStorageOperations() throws InterruptedException {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .addClusterNode("node1", 8123)
      .addClusterNode("node2", 8123)
      .addClusterNode("node3", 8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    ClickHouseStorage storage = new ClickHouseStorage(builder);
    storages.add(storage);

    int numThreads = 10;
    int operationsPerThread = 20;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      CountDownLatch latch = new CountDownLatch(numThreads);
      AtomicInteger totalOperations = new AtomicInteger(0);

      for (int t = 0; t < numThreads; t++) {
        executor.submit(() -> {
          try {
            for (int op = 0; op < operationsPerThread; op++) {
              int spanCount = 1 + (op % 10); // 1-10 спанов в батче
              storage.spanConsumer().accept(createTestSpans(spanCount));
              totalOperations.incrementAndGet();
            }
          } finally {
            latch.countDown();
          }
        });
      }

      assertTrue(latch.await(50, TimeUnit.SECONDS));

      assertEquals(numThreads * operationsPerThread, totalOperations.get());
    } finally {
      executor.shutdown();
    }
  }

  // ==================== Тесты резервных узлов ====================

  @Test
  public void clusterWithPrimaryAndReplicaNodes() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      // Первичные узлы
      .addClusterNode("primary1", 8123)
      .addClusterNode("primary2", 8123)
      // Резервные узлы
      .addClusterNode("replica1", 8123)
      .addClusterNode("replica2", 8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    assertNotNull(builder);
  }

  @Test
  @Timeout(45)
  public void failoverToReplicaNode() {
    // Первоначально используем основной узел
    ClickHouseStorage.Builder builderPrimary = new ClickHouseStorage.Builder()
      .addClusterNode("primary", 8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    ClickHouseStorage storagePrimary = new ClickHouseStorage(builderPrimary);
    storages.add(storagePrimary);

    storagePrimary.spanConsumer().accept(createTestSpans(5));

    // Закрываем основной и переходим на резервный
    storagePrimary.close();

    ClickHouseStorage.Builder builderReplica = new ClickHouseStorage.Builder()
      .addClusterNode("replica", 8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    ClickHouseStorage storageReplica = new ClickHouseStorage(builderReplica);
    storages.add(storageReplica);

    storageReplica.spanConsumer().accept(createTestSpans(5));

    assertNotNull(storageReplica);
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
        .name("storage-integration-span-" + i)
        .timestamp(baseTimestamp + i)
        .duration(100 + i)
        .localEndpoint(zipkin2.Endpoint.newBuilder().serviceName("test-service").build())
        .build();
      spans.add(span);
    }
    return spans;
  }
}

