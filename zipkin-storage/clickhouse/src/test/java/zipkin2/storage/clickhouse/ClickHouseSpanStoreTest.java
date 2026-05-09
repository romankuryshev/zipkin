package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class ClickHouseSpanStoreTest {

  @Test void strictTraceIdTrueIsStored() {
    var store = new ClickHouseSpanStore(mock(Client.class), "zipkin", true);
    assertTrue(store.isStrictTraceId());
  }

  @Test void strictTraceIdFalseIsStored() {
    var store = new ClickHouseSpanStore(mock(Client.class), "zipkin", false);
    assertFalse(store.isStrictTraceId());
  }

  @Test void includeSpanStatisticsTrueIsStored() {
    var store = new ClickHouseSpanStore(mock(Client.class), "zipkin", true, 100, true);
    assertTrue(store.isIncludeSpanStatistics());
  }

  @Test void includeSpanStatisticsFalseIsStored() {
    var store = new ClickHouseSpanStore(mock(Client.class), "zipkin", true, 100, false);
    assertFalse(store.isIncludeSpanStatistics());
  }
}
