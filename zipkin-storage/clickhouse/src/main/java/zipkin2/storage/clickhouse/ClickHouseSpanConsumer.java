package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.clickhouse.call.InsertSpansCall;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class ClickHouseSpanConsumer implements SpanConsumer {
  private static final Logger log = LoggerFactory.getLogger(ClickHouseSpanConsumer.class);

  private final Client client;
  private final String database;
  private final boolean strictTraceId;

  private static final int BATCH_SIZE = 10;
  private static final int AUTO_FLUSH_INTERVAL_MS = 5000;

  private final Queue<Span> buffer = new ConcurrentLinkedQueue<>();
  private final ScheduledExecutorService scheduler;
  private final ReentrantLock lock = new ReentrantLock();

  public ClickHouseSpanConsumer(Client client, String database, boolean strictTraceId) {
    this.client = client;
    this.database = database;
    this.strictTraceId = strictTraceId;
    this.scheduler = Executors.newScheduledThreadPool(1, r -> {
      Thread t = new Thread(r, "ClickHouseBatchFlush");
      t.setDaemon(true);
      return t;
    });
    scheduler.scheduleWithFixedDelay(this::flushBufferAsync, 30000, AUTO_FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public Call<Void> accept(List<Span> spans) {
    if (spans == null || spans.isEmpty()) {
      return Call.create(null);
    }

    buffer.addAll(spans);

    // Check if we should flush immediately (batch size reached)
    if (buffer.size() >= BATCH_SIZE) {
      return flushBuffer();
    }
    return Call.create(null);
  }

  private Call<Void> flushBuffer() {
    lock.lock();
    try {
      if (buffer.isEmpty()) {
        return Call.create(null);
      }
      List<Span> toFlush = new ArrayList<>(buffer);
      buffer.clear();
      return new InsertSpansCall(client, database, toFlush, strictTraceId);
    } finally {
      lock.unlock();
    }
  }

  private void flushBufferAsync() {
    if (!buffer.isEmpty()) {
      try {
        Call<Void> call = flushBuffer();
        call.execute();
        log.debug("Buffer flushed successfully");
      } catch (Exception e) {
        log.error("Error flushing buffer to ClickHouse", e);
      }
    }
  }

  public void close() {
    try {
      // Flush remaining spans
      flushBufferAsync();

      // Shutdown scheduler
      scheduler.shutdown();
      if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
        log.warn("Scheduler did not terminate within 30 seconds, forcing shutdown");
        scheduler.shutdownNow();
      }
      log.info("ClickHouseSpanConsumer shut down successfully");
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      log.error("Error during ClickHouseSpanConsumer shutdown", e);
    }
  }
}
