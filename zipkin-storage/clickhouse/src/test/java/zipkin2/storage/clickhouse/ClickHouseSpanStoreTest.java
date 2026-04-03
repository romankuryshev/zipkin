package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import org.junit.jupiter.api.Test;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStatistics;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ClickHouseSpanStoreTest {

  @Test
  public void getTracesReturnsCallInstance() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    var request = QueryRequest.newBuilder().build();
    Call<List<List<Span>>> result = spanStore.getTraces(request);

    assertNotNull(result);
  }

  @Test
  public void getTraceReturnsCallInstance() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    Call<List<Span>> result = spanStore.getTrace("0000000000000001");

    assertNotNull(result);
  }

  @Test
  public void getTraceWithValidTraceId() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    Call<List<Span>> result = spanStore.getTrace("0000000000000123");

    assertNotNull(result);
  }

  @Test
  public void getServiceNamesReturnsCallInstance() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    Call<List<String>> result = spanStore.getServiceNames();

    assertNotNull(result);
  }

  @Test
  public void getSpanNamesReturnsCallInstance() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    Call<List<String>> result = spanStore.getSpanNames("order-db");

    assertNotNull(result);
  }

  @Test
  public void getSpanNamesWithDifferentServiceNames() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    Call<List<String>> result1 = spanStore.getSpanNames("service-1");
    Call<List<String>> result2 = spanStore.getSpanNames("service-2");

    assertNotNull(result1);
    assertNotNull(result2);
    assertNotEquals(result1, result2);
  }

  @Test
  public void getDependenciesReturnsCallInstance() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    Call<List<DependencyLink>> result = spanStore.getDependencies(System.currentTimeMillis() * 1000, 3600000);

    assertNotNull(result);
  }

  @Test
  public void getDependenciesWithDifferentTimestamps() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    long now = System.currentTimeMillis() * 1000;
    Call<List<DependencyLink>> result1 = spanStore.getDependencies(now, 3600000);
    Call<List<DependencyLink>> result2 = spanStore.getDependencies(now - 86400000, 86400000);

    assertNotNull(result1);
    assertNotNull(result2);
  }

  @Test
  public void getSpanStatisticsReturnsCallInstance() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    long endTs = System.currentTimeMillis() * 1000;
    Call<SpanStatistics> result = spanStore.getSpanStatistics("order-db", "select-orders", "CLIENT", endTs, 3600000);

    assertNotNull(result);
  }

  @Test
  public void getSpanStatisticsWithDifferentKinds() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    long endTs = System.currentTimeMillis() * 1000;
    Call<SpanStatistics> result1 = spanStore.getSpanStatistics("service", "span", "CLIENT", endTs, 3600000);
    Call<SpanStatistics> result2 = spanStore.getSpanStatistics("service", "span", "SERVER", endTs, 3600000);

    assertNotNull(result1);
    assertNotNull(result2);
  }

  @Test
  public void isStrictTraceIdTrue() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    assertTrue(spanStore.isStrictTraceId());
  }

  @Test
  public void isStrictTraceIdFalse() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", false);

    assertFalse(spanStore.isStrictTraceId());
  }

  @Test
  public void getTracesWithEmptyQueryRequest() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    var request = QueryRequest.newBuilder().build();
    Call<List<List<Span>>> result = spanStore.getTraces(request);

    assertNotNull(result);
  }

  @Test
  public void getTracesWithServiceNameFilter() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    var request = QueryRequest.newBuilder()
      .serviceName("order-db")
      .build();
    Call<List<List<Span>>> result = spanStore.getTraces(request);

    assertNotNull(result);
  }

  @Test
  public void getTracesWithSpanNameFilter() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    var request = QueryRequest.newBuilder()
      .spanName("select-orders")
      .build();
    Call<List<List<Span>>> result = spanStore.getTraces(request);

    assertNotNull(result);
  }

  @Test
  public void getTracesWithMultipleFilters() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    var request = QueryRequest.newBuilder()
      .serviceName("order-db")
      .spanName("select-orders")
      .limit(100)
      .build();
    Call<List<List<Span>>> result = spanStore.getTraces(request);

    assertNotNull(result);
  }

  @Test
  public void constructorWithDifferentDatabase() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "custom_db", true);

    assertNotNull(spanStore);
  }

  @Test
  public void getTraceWithNullTraceId() {
    var mockClient = mock(Client.class);
    var spanStore = new ClickHouseSpanStore(mockClient, "zipkin", true);

    Call<List<Span>> result = spanStore.getTrace(null);

    assertNotNull(result);
  }
}

