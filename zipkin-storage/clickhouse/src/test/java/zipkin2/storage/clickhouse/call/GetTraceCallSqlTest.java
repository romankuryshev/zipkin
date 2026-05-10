package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class GetTraceCallSqlTest {

  private Client client;
  private ArgumentCaptor<String> sqlCaptor;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    client = mock(Client.class);
    QueryResponse mockResponse = mock(QueryResponse.class);
    ClickHouseBinaryFormatReader mockReader = mock(ClickHouseBinaryFormatReader.class);

    when(client.query(anyString(), anyMap(), any(QuerySettings.class)))
      .thenReturn(CompletableFuture.completedFuture(mockResponse));
    when(client.newBinaryFormatReader(mockResponse)).thenReturn(mockReader);
    when(mockReader.hasNext()).thenReturn(false);

    sqlCaptor = ArgumentCaptor.forClass(String.class);
  }

  private String capturedSql(GetTraceCall call) {
    call.doExecute();
    verify(client).query(sqlCaptor.capture(), anyMap(), any(QuerySettings.class));
    return sqlCaptor.getValue();
  }

  @Test
  void withStatistics_selectsStatsDurationColumns() {
    String sql = capturedSql(new GetTraceCall(client, "zipkin", "abc1", true));
    assertTrue(sql.contains("stats.median_duration"));
    assertTrue(sql.contains("stats.average_duration"));
    assertTrue(sql.contains("stats.p50"));
    assertTrue(sql.contains("stats.p95"));
    assertTrue(sql.contains("stats.p99"));
  }

  @Test
  void withStatistics_selectsStatsCountColumns() {
    String sql = capturedSql(new GetTraceCall(client, "zipkin", "abc1", true));
    assertTrue(sql.contains("stats.success_count"));
    assertTrue(sql.contains("stats.error_count"));
    assertTrue(sql.contains("stats.total_count"));
  }

  @Test
  void withStatistics_includesLeftJoinFragment() {
    String sql = capturedSql(new GetTraceCall(client, "zipkin", "abc1", true));
    assertTrue(sql.contains("LEFT JOIN"));
    assertTrue(sql.contains("spans_aggregate_stats"));
  }

  @Test
  void withStatistics_joinsOnNameKindServiceName() {
    String sql = capturedSql(new GetTraceCall(client, "zipkin", "abc1", true));
    assertTrue(sql.contains("s.name = stats.span_name"));
    assertTrue(sql.contains("s.kind = stats.span_kind"));
    assertTrue(sql.contains("s.local_endpoint_service_name = stats.service_name"));
  }

  @Test
  void withStatistics_usesCorrectDatabaseInJoin() {
    String sql = capturedSql(new GetTraceCall(client, "tracing", "abc1", true));
    assertTrue(sql.contains("tracing.spans_aggregate_stats"));
  }

  @Test
  void withStatistics_baseColumnsAlwaysPresent() {
    String sql = capturedSql(new GetTraceCall(client, "zipkin", "abc1", true));
    assertTrue(sql.contains("s.trace_id"));
    assertTrue(sql.contains("s.span_id"));
    assertTrue(sql.contains("s.name"));
    assertTrue(sql.contains("s.annotations"));
    assertTrue(sql.contains("s.tags"));
  }

  @Test
  void withStatistics_whereClausePresent() {
    String sql = capturedSql(new GetTraceCall(client, "zipkin", "abc1", true));
    assertTrue(sql.contains("WHERE s.trace_id = {traceIdLow:UInt64}"));
    assertTrue(sql.contains("s.trace_id_high = {traceIdHigh:UInt64}"));
  }

  @Test
  void withStatistics_orderByPresent() {
    String sql = capturedSql(new GetTraceCall(client, "zipkin", "abc1", true));
    assertTrue(sql.contains("ORDER BY s.timestamp ASC"));
  }

  @Test
  void withoutStatistics_doesNotSelectStatsColumns() {
    String sql = capturedSql(new GetTraceCall(client, "zipkin", "abc1", false));
    assertFalse(sql.contains("stats.median_duration"));
    assertFalse(sql.contains("stats.p50"));
  }

  @Test
  void withoutStatistics_doesNotIncludeLeftJoin() {
    String sql = capturedSql(new GetTraceCall(client, "zipkin", "abc1", false));
    assertFalse(sql.contains("LEFT JOIN"));
  }
}
