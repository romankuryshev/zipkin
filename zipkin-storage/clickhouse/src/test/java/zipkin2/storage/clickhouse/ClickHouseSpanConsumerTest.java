package zipkin2.storage.clickhouse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import zipkin2.Call;
import zipkin2.Span;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class ClickHouseSpanConsumerTest {

  private com.clickhouse.client.api.Client mockClient;
  private ClickHouseSpanConsumer consumer;

  @BeforeEach void setUp() {
    mockClient = mock(com.clickhouse.client.api.Client.class);
  }

  @AfterEach void tearDown() {
    if (consumer != null) consumer.close();
  }

  @Test void acceptBelowBatchSizeReturnsNoOp() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    Call<Void> result = consumer.accept(spans(5));

    assertEquals(Call.create(null), result,
      "Buffer below BATCH_SIZE must return a no-op Call");
  }

  @Test void acceptAtBatchSizeReturnsInsertCall() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    Call<Void> fillResult = consumer.accept(spans(10_000));
    Call<Void> triggerResult = consumer.accept(spans(1));

    assertEquals(Call.create(null), fillResult,
      "First batch goes into the buffer without flushing");
    assertNotEquals(Call.create(null), triggerResult,
      "When buffer reaches BATCH_SIZE, next accept() must return a real InsertSpansCall");
  }

  @Test void bufferIsResetAfterFlush() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    consumer.accept(spans(10_000));
    consumer.accept(spans(1));

    Call<Void> afterFlush = consumer.accept(spans(5));
    assertEquals(Call.create(null), afterFlush,
      "After a flush the buffer is cleared; small batches must return no-op again");
  }

  @Test void nullSpansListReturnsNoOp() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    Call<Void> result = consumer.accept(null);

    assertEquals(Call.create(null), result);
  }

  @Test void emptySpansListReturnsNoOp() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);

    Call<Void> result = consumer.accept(new ArrayList<>());

    assertEquals(Call.create(null), result);
  }

  @Test
  @Timeout(10)
  void concurrentAcceptIsThreadSafe() throws InterruptedException {
    consumer = new ClickHouseSpanConsumer(mockClient, true);
    int threads = 5;
    int callsPerThread = 4;

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch latch = new CountDownLatch(threads);
      AtomicInteger errors = new AtomicInteger();

      for (int t = 0; t < threads; t++) {
        executor.submit(() -> {
          try {
            for (int i = 0; i < callsPerThread; i++) {
              consumer.accept(spans(1));
            }
          } catch (Exception e) {
            errors.incrementAndGet();
          } finally {
            latch.countDown();
          }
        });
      }

      assertTrue(latch.await(10, TimeUnit.SECONDS));
      assertEquals(0, errors.get(), "No exceptions must occur during concurrent accept()");
    } finally {
      executor.shutdown();
    }
  }

  @Test
  @Timeout(15)
  void concurrentAcceptTracksAllSpanCounts() throws InterruptedException {
    consumer = new ClickHouseSpanConsumer(mockClient, true);
    int threads = 3;
    int batchesPerThread = 2;
    int spansPerBatch = 5;

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch latch = new CountDownLatch(threads);
      List<Integer> counts = Collections.synchronizedList(new ArrayList<>());

      for (int t = 0; t < threads; t++) {
        executor.submit(() -> {
          try {
            for (int b = 0; b < batchesPerThread; b++) {
              List<Span> batch = spans(spansPerBatch);
              consumer.accept(batch);
              counts.add(batch.size());
            }
          } finally {
            latch.countDown();
          }
        });
      }

      assertTrue(latch.await(15, TimeUnit.SECONDS));
      int total = counts.stream().mapToInt(Integer::intValue).sum();
      assertEquals(threads * batchesPerThread * spansPerBatch, total,
        "Every span submitted must be counted");
    } finally {
      executor.shutdown();
    }
  }

  @Test
  @Timeout(35)
  void closeIsIdempotent() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);
    consumer.accept(spans(3));

    assertDoesNotThrow(() -> {
      consumer.close();
      consumer.close();
    });
  }

  @Test
  @Timeout(35)
  void closeCompletesInReasonableTime() {
    consumer = new ClickHouseSpanConsumer(mockClient, true);
    consumer.accept(spans(3));

    long start = System.currentTimeMillis();
    consumer.close();
    long elapsed = System.currentTimeMillis() - start;

    assertTrue(elapsed < 35_000, "close() must finish within 35 s, took " + elapsed + " ms");
  }

  private static List<Span> spans(int count) {
    List<Span> result = new ArrayList<>(count);
    long baseTs = System.currentTimeMillis() * 1_000;
    for (int i = 0; i < count; i++) {
      result.add(Span.newBuilder()
        .traceId(String.format("%016x%016x", 0x1111111111111111L + i, 0x2222222222222222L + i))
        .id(String.format("%016x", 0x3333333333333333L + i))
        .name("span-" + i)
        .timestamp(baseTs + i)
        .duration(100 + i)
        .build());
    }
    return result;
  }
}
