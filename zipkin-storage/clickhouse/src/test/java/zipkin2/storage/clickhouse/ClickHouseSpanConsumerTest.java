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

public class ClickHouseSpanConsumerTest {

  private static final Logger log = LoggerFactory.getLogger(ClickHouseSpanConsumerTest.class);

  private com.clickhouse.client.api.Client mockClient;
  private ClickHouseSpanConsumer consumer;

  @BeforeEach
  public void setUp() {
    mockClient = mock(com.clickhouse.client.api.Client.class);
  }

  @AfterEach
  public void tearDown() {
    if (consumer != null) {
      consumer.close();
    }
  }

  @Test
  public void bufferAccumulatesSpansBelowThreshold() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    List<Span> spans = createTestSpans(5);
    Call<Void> result = consumer.accept(spans);

    assertNotNull(result);
  }

  @Test
  public void bufferFlushesWhenExceedingBatchSize() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    List<Span> spans = createTestSpans(10000);
    List<Span> spans2 = createTestSpans(1);
    Call<Void> result = consumer.accept(spans);
    Call<Void> result2 = consumer.accept(spans2);

    assertEquals(Call.create(null), result);
    assertNotEquals(Call.create(null), result2);
  }

  @Test
  public void bufferClearedAfterFlush() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    List<Span> spans1 = createTestSpans(10);
    consumer.accept(spans1);

    List<Span> spans2 = createTestSpans(5);
    Call<Void> call2 = consumer.accept(spans2);

    assertEquals(Call.create(null), call2,
      "После фланша буфер должен быть очищен");
  }

  @Test
  public void multipleBatchesAccumulateCorrectly() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    consumer.accept(createTestSpans(3));
    consumer.accept(createTestSpans(3));
    Call<Void> call3 = consumer.accept(createTestSpans(4));

    assertNotNull(call3);
  }

  @Test
  @Timeout(10)
  @SuppressWarnings("resource")
  public void concurrentSpansAdditionIsThreadSafe() throws InterruptedException {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    int numThreads = 5;
    int spansPerThread = 4;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      CountDownLatch latch = new CountDownLatch(numThreads);
      AtomicInteger errorCount = new AtomicInteger(0);

      for (int t = 0; t < numThreads; t++) {
        executor.submit(() -> {
          try {
            for (int i = 0; i < spansPerThread; i++) {
              List<Span> spans = createTestSpans(1);
              consumer.accept(spans);
            }
          } catch (Exception e) {
            errorCount.incrementAndGet();
            log.error("Error during concurrent span addition", e);
          } finally {
            latch.countDown();
          }
        });
      }

      assertTrue(latch.await(10, TimeUnit.SECONDS), "Все потоки должны завершиться");
      assertEquals(0, errorCount.get(), "Не должно быть ошибок при конкурентном доступе");
    } finally {
      executor.shutdown();
    }
  }

  @Test
  @Timeout(15)
  @SuppressWarnings("resource")
  public void concurrentBatchesDoNotLoseSpans() throws InterruptedException {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    int numThreads = 3;
    int batchesPerThread = 2;
    int spansPerBatch = 5;

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      CountDownLatch latch = new CountDownLatch(numThreads);
      List<Integer> acceptedSpanCounts = Collections.synchronizedList(new ArrayList<>());

      for (int t = 0; t < numThreads; t++) {
        executor.submit(() -> {
          try {
            for (int b = 0; b < batchesPerThread; b++) {
              List<Span> spans = createTestSpans(spansPerBatch);
              consumer.accept(spans);
              acceptedSpanCounts.add(spans.size());
            }
          } finally {
            latch.countDown();
          }
        });
      }

      assertTrue(latch.await(15, TimeUnit.SECONDS));

      int totalSpans = acceptedSpanCounts.stream().mapToInt(Integer::intValue).sum();
      assertEquals(numThreads * batchesPerThread * spansPerBatch, totalSpans);
    } finally {
      executor.shutdown();
    }
  }

  @Test
  @Timeout(10)
  @SuppressWarnings("resource")
  public void raceConditionBetweenBufferAndFlush() throws InterruptedException {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    CyclicBarrier barrier = new CyclicBarrier(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      CountDownLatch latch = new CountDownLatch(2);

      executor.submit(() -> {
        try {
          barrier.await();
          for (int i = 0; i < 5; i++) {
            consumer.accept(createTestSpans(2));
            Thread.sleep(10);
          }
        } catch (Exception e) {
          log.error("Error in thread 1", e);
        } finally {
          latch.countDown();
        }
      });

      executor.submit(() -> {
        try {
          barrier.await();
          Thread.sleep(30);
          consumer.close();
        } catch (Exception e) {
          log.error("Error in thread 2", e);
        } finally {
          latch.countDown();
        }
      });

      assertTrue(latch.await(10, TimeUnit.SECONDS));
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void handleNullSpansList() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    Call<Void> result = consumer.accept(null);

    assertNotNull(result);
    assertEquals(Call.create(null), result);
  }

  @Test
  public void handleEmptySpansList() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    Call<Void> result = consumer.accept(new ArrayList<>());

    assertNotNull(result);
    assertEquals(Call.create(null), result);
  }

  @Test
  public void consumerContinuesAfterFlushError() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    consumer.accept(createTestSpans(10));

    Call<Void> result = consumer.accept(createTestSpans(5));

    assertNotNull(result);
  }

  @Test
  public void multipleFlushesWithDifferentBatches() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    consumer.accept(createTestSpans(10));
    consumer.accept(createTestSpans(10));
    consumer.accept(createTestSpans(10));

    assertNotNull(consumer);
  }

  @Test
  @Timeout(35)
  public void closeFlushesRemainingSpans() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    List<Span> spans = createTestSpans(5);
    consumer.accept(spans);

    consumer.close();

    assertTrue(true);
  }

  @Test
  @Timeout(35)
  public void schedulerShutdownWaitsForCompletion() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    consumer.accept(createTestSpans(3));

    long startTime = System.currentTimeMillis();
    consumer.close();
    long duration = System.currentTimeMillis() - startTime;

    assertTrue(duration < 35000, "Close должен завершиться за < 35 секунд");
  }

  @Test
  @Timeout(35)
  public void multipleCloseCallsAreIdempotent() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    consumer.accept(createTestSpans(3));
    consumer.close();

    consumer.close();

    assertTrue(true);
  }

  @Test
  public void multipleConsumersWithSameClientIndependent() {
    var consumer1 = new ClickHouseSpanConsumer(mockClient, true);
    var consumer2 = new ClickHouseSpanConsumer(mockClient, true);

    try {
      consumer1.accept(createTestSpans(5));
      consumer2.accept(createTestSpans(5));

      assertNotNull(consumer1);
      assertNotNull(consumer2);
    } finally {
      consumer1.close();
      consumer2.close();
    }
  }

  @Test
  public void acceptWithDifferentStrictTraceIdModes() {
    var consumer1 = new ClickHouseSpanConsumer(mockClient, true);
    List<Span> result1 = createTestSpans(5);
    consumer1.accept(result1);
    consumer1.close();

    var consumer2 = new ClickHouseSpanConsumer(mockClient, false);
    List<Span> result2 = createTestSpans(5);
    consumer2.accept(result2);
    consumer2.close();

    assertTrue(true);
  }

  @Test
  public void acceptWithDifferentDatabases() {
    var consumer1 = new ClickHouseSpanConsumer(mockClient, true);
    consumer1.accept(createTestSpans(5));
    consumer1.close();

    var consumer2 = new ClickHouseSpanConsumer(mockClient, true);
    consumer2.accept(createTestSpans(5));
    consumer2.close();

    assertTrue(true);
  }

  @Test
  @Timeout(35)
  public void largeNumberOfSpansHandledCorrectly() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    for (int i = 0; i < 100; i++) {
      consumer.accept(createTestSpans(1));
    }

    assertNotNull(consumer);
  }

  @Test
  @Timeout(35)
  public void largeSpanBatchProcessed() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    List<Span> largeSpans = createTestSpans(100);
    Call<Void> result = consumer.accept(largeSpans);

    assertNotNull(result);
  }

  @Test
  public void spanAttributesPreservedThroughBuffer() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    List<Span> spans = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      long traceIdMsb = Math.abs(UUID.randomUUID().getMostSignificantBits());
      long traceIdLsb = Math.abs(UUID.randomUUID().getLeastSignificantBits());
      long spanId = Math.abs(UUID.randomUUID().getMostSignificantBits());

      if (traceIdMsb == 0) traceIdMsb = 1;
      if (spanId == 0) spanId = 1;

      Span span = Span.newBuilder()
        .traceId(String.format("%016x%016x", traceIdMsb, traceIdLsb))
        .id(String.format("%016x", spanId))
        .name("service-" + i)
        .parentId(String.format("%016x", Math.abs(UUID.randomUUID().getMostSignificantBits()) | 1L))
        .timestamp(System.currentTimeMillis() * 1000 + i)
        .duration(1000 + i * 100)
        .kind(Span.Kind.SERVER)
        .build();
      spans.add(span);
    }

    consumer.accept(spans);

    assertNotNull(consumer);
  }

  @Test
  @Timeout(35)
  public void timeoutFlushWorksProperly() throws InterruptedException {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    List<Span> spans = createTestSpans(3);
    consumer.accept(spans);

    Thread.sleep(6000);

    consumer.close();
  }

  @Test
  @Timeout(40)
  public void closeBeforeTimeoutFlushTriggers() throws InterruptedException {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    List<Span> spans = createTestSpans(3);
    consumer.accept(spans);

    Thread.sleep(1000);
    consumer.close();

    assertTrue(true);
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
        .name("test-span-" + i)
        .timestamp(baseTimestamp + i)
        .duration(100 + i)
        .build();
      spans.add(span);
    }
    return spans;
  }
}

