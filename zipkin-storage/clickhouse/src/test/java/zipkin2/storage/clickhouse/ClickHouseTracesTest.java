package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import org.junit.jupiter.api.Test;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.clickhouse.call.GetTraceCall;
import zipkin2.storage.clickhouse.call.GetTracesByIdCall;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ClickHouseTracesTest {

  @Test
  public void getTraceReturnsCallInstance() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");

    Call<List<Span>> result = traces.getTrace("0000000000000001");

    assertNotNull(result);
    assertInstanceOf(GetTraceCall.class, result);
  }

  @Test
  public void getTraceWithValidTraceId() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");

    Call<List<Span>> result = traces.getTrace("0000000000000123");

    assertNotNull(result);
  }

  @Test
  public void getTraceWithHighTraceId() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");

    Call<List<Span>> result = traces.getTrace("00000000000000010000000000000001");

    assertNotNull(result);
  }

  @Test
  public void getTracesReturnsCallInstance() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");
    List<String> traceIds = List.of("0000000000000001", "0000000000000002");

    Call<List<List<Span>>> result = traces.getTraces(traceIds);

    assertNotNull(result);
    assertInstanceOf(GetTracesByIdCall.class, result);
  }

  @Test
  public void getTracesWithMultipleTraceIds() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");
    List<String> traceIds = List.of(
      "0000000000000001",
      "0000000000000002",
      "0000000000000003"
    );

    Call<List<List<Span>>> result = traces.getTraces(traceIds);

    assertNotNull(result);
  }

  @Test
  public void getTracesWithSingleTraceId() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");
    List<String> traceIds = List.of("0000000000000001");

    Call<List<List<Span>>> result = traces.getTraces(traceIds);

    assertNotNull(result);
  }

  @Test
  public void getTracesWithEmptyTraceIdsList() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");
    List<String> traceIds = List.of();

    Call<List<List<Span>>> result = traces.getTraces(traceIds);

    assertNotNull(result);
  }

  @Test
  public void getTracesWithLargeTraceIdsList() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");
    List<String> traceIds = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      traceIds.add(String.format("%016x", i));
    }

    Call<List<List<Span>>> result = traces.getTraces(traceIds);

    assertNotNull(result);
  }

  @Test
  public void constructorStoresClientAndDatabase() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");

    String result = traces.toString();

    assertTrue(result.contains("zipkin"));
  }

  @Test
  public void constructorWithDifferentDatabase() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "custom_db");

    String result = traces.toString();

    assertTrue(result.contains("custom_db"));
  }

  @Test
  public void toStringContainsClassName() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");

    String result = traces.toString();

    assertTrue(result.contains("ClickHouseTraces"));
  }

  @Test
  public void multipleGetTraceCallsWithDifferentIds() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");

    Call<List<Span>> result1 = traces.getTrace("0000000000000001");
    Call<List<Span>> result2 = traces.getTrace("0000000000000002");
    Call<List<Span>> result3 = traces.getTrace("0000000000000003");

    assertNotNull(result1);
    assertNotNull(result2);
    assertNotNull(result3);
    assertNotEquals(result1, result2);
    assertNotEquals(result2, result3);
  }

  @Test
  public void multipleGetTracesCallsWithDifferentIdsList() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");

    Call<List<List<Span>>> result1 = traces.getTraces(List.of("0000000000000001"));
    Call<List<List<Span>>> result2 = traces.getTraces(List.of("0000000000000002", "0000000000000003"));

    assertNotNull(result1);
    assertNotNull(result2);
    assertNotEquals(result1, result2);
  }

  @Test
  public void getTracesWithIterableImplementation() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");
    Iterable<String> traceIds = () -> List.of("0000000000000001", "0000000000000002").iterator();

    Call<List<List<Span>>> result = traces.getTraces(traceIds);

    assertNotNull(result);
  }

  @Test
  public void getTracesWithDifferentDatabases() {
    var mockClient = mock(Client.class);
    var traces1 = new ClickHouseTraces(mockClient, "zipkin");
    var traces2 = new ClickHouseTraces(mockClient, "custom_db");

    Call<List<List<Span>>> result1 = traces1.getTraces(List.of("0000000000000001"));
    Call<List<List<Span>>> result2 = traces2.getTraces(List.of("0000000000000001"));

    assertNotNull(result1);
    assertNotNull(result2);
  }

  @Test
  public void getTracesReturnsNewInstanceEachTime() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");
    List<String> traceIds = List.of("0000000000000001");

    Call<List<List<Span>>> result1 = traces.getTraces(traceIds);
    Call<List<List<Span>>> result2 = traces.getTraces(traceIds);

    assertNotNull(result1);
    assertNotNull(result2);
    assertNotSame(result1, result2);
  }

  @Test
  public void getTraceReturnsNewInstanceEachTime() {
    var mockClient = mock(Client.class);
    var traces = new ClickHouseTraces(mockClient, "zipkin");

    Call<List<Span>> result1 = traces.getTrace("0000000000000001");
    Call<List<Span>> result2 = traces.getTrace("0000000000000001");

    assertNotNull(result1);
    assertNotNull(result2);
    assertNotSame(result1, result2);
  }
}
