package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.clickhouse.cache.AutocompleteTagsCache;
import zipkin2.storage.clickhouse.call.InsertSpansCall;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class ClickHouseSpanConsumer implements SpanConsumer {
  private static final Logger log = LoggerFactory.getLogger(ClickHouseSpanConsumer.class);

  private final Client client;
  private final boolean strictTraceId;
  private final Set<String> autocompleteKeys;
  private final AutocompleteTagsCache autocompleteTagsCache;

  private final int batchSize;
  private final int autoFlushIntervalMs;

  private Queue<Span> buffer = new ConcurrentLinkedQueue<>();
  private final ScheduledExecutorService scheduler;
  private final ReentrantLock lock = new ReentrantLock();

  public ClickHouseSpanConsumer(Client client, boolean strictTraceId) {
    this(client, strictTraceId, Set.of(),
      new AutocompleteTagsCache((int) TimeUnit.HOURS.toMillis(1), 5 * 4000, Set.of()));
  }

  public ClickHouseSpanConsumer(Client client, boolean strictTraceId,
                                Set<String> autocompleteKeys,
                                AutocompleteTagsCache autocompleteTagsCache) {
    this(client, strictTraceId, autocompleteKeys, autocompleteTagsCache, 10000, 5000);
  }

  public ClickHouseSpanConsumer(Client client, boolean strictTraceId,
                                Set<String> autocompleteKeys,
                                AutocompleteTagsCache autocompleteTagsCache,
                                int batchSize,
                                int autoFlushIntervalMs) {
    this.client = client;
    this.strictTraceId = strictTraceId;
    this.autocompleteKeys = autocompleteKeys;
    this.autocompleteTagsCache = autocompleteTagsCache;
    this.batchSize = batchSize;
    this.autoFlushIntervalMs = autoFlushIntervalMs;
    this.scheduler = Executors.newScheduledThreadPool(1, r -> {
      Thread t = new Thread(r, "ClickHouseBatchFlush");
      t.setDaemon(true);
      return t;
    });
    scheduler.scheduleWithFixedDelay(this::flushBufferAsync, 30000, this.autoFlushIntervalMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public Call<Void> accept(List<Span> spans) {
    if (spans == null || spans.isEmpty()) {
      return Call.create(null);
    }


    Call<Void> insertCall = Call.create(null);
    if (buffer.size() >= batchSize) {
      insertCall = flushBuffer();
    }
    buffer.addAll(spans);
    return insertCall;
  }

  private Call<Void> flushBuffer() {
    lock.lock();
    try {
      if (buffer.size() < batchSize) {
        return Call.create(null);
      }
      List<Span> toFlush = new ArrayList<>(buffer);
      buffer = new ConcurrentLinkedQueue<>();
      return new InsertSpansCall(client, toFlush, strictTraceId, autocompleteKeys, autocompleteTagsCache);
    } finally {
      lock.unlock();
    }
  }

  private void flushBufferAsync() {
    if (!buffer.isEmpty()) {
      try {
        forceFlush();
        log.debug("Buffer flushed successfully");
      } catch (Exception e) {
        log.error("Error flushing buffer to ClickHouse", e);
      }
    }
  }

  void forceFlush() throws java.io.IOException {
    lock.lock();
    try {
      if (buffer.isEmpty()) return;
      List<Span> toFlush = new ArrayList<>(buffer);
      buffer = new ConcurrentLinkedQueue<>();
      new InsertSpansCall(client, toFlush, strictTraceId, autocompleteKeys, autocompleteTagsCache).execute();
    } finally {
      lock.unlock();
    }
  }

  public void close() {
    try {
      flushBufferAsync();
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
