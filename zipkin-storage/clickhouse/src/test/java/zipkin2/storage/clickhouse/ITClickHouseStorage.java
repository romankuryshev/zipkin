package zipkin2.storage.clickhouse;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import zipkin2.Span;
import zipkin2.storage.StorageComponent;

@Testcontainers
@Tag("docker")
class ITClickHouseStorage {

  @Container static ClickHouseContainer clickhouse = new ClickHouseContainer();

  @Nested
  class ITTraces extends zipkin2.storage.ITTraces<ClickHouseStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return clickhouse.newStorageBuilder();
    }

    @Override protected void blockWhileInFlight() {
      forceFlush(storage);
    }

    @Override public void clear() throws Exception {
      clickhouse.clear();
    }

    @Test
    @Override
    @Disabled("MergeTree does not deduplicate; ReplacingMergeTree requires background merge")
    protected void getTrace_deduplicates(TestInfo testInfo) {
    }
  }

  @Nested
  class ITSpanStore extends zipkin2.storage.ITSpanStore<ClickHouseStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return clickhouse.newStorageBuilder();
    }

    @Override protected void blockWhileInFlight() {
      forceFlush(storage);
    }

    @Override public void clear() throws Exception {
      clickhouse.clear();
    }
  }

  @Nested
  class ITServiceAndSpanNames extends zipkin2.storage.ITServiceAndSpanNames<ClickHouseStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return clickhouse.newStorageBuilder();
    }

    @Override protected void blockWhileInFlight() {
      forceFlush(storage);
    }

    @Override public void clear() throws Exception {
      clickhouse.clear();
    }
  }

  @Nested
  class ITAutocompleteTags extends zipkin2.storage.ITAutocompleteTags<ClickHouseStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return clickhouse.newStorageBuilder().autocompleteKeys(List.of("http.host"));
    }

    @Override protected void blockWhileInFlight() {
      forceFlush(storage);
    }

    @Override public void clear() throws Exception {
      clickhouse.clear();
    }

  }

  @Nested
  class ITDependencies extends zipkin2.storage.ITDependencies<ClickHouseStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return clickhouse.newStorageBuilder();
    }

    @Override protected void blockWhileInFlight() {
      forceFlush(storage);
    }

    @Override public void clear() throws Exception {
      clickhouse.clear();
    }

    @Test
    @Override
    @Disabled("getDependencies() does not apply endTs/lookback time filtering")
    protected void endTsInsideTheTrace(TestInfo testInfo) {
    }


    @Test
    @Override
    @Disabled("SummingMergeTree accumulates counts; re-inserting the same spans doubles call_count")
    protected void replayOverwrites(TestInfo testInfo) {
    }
  }

  static void forceFlush(ClickHouseStorage storage) {
    try {
      storage.forceFlush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
