package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QuerySettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import zipkin2.storage.QueryRequest;

import java.math.BigInteger;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class GetTracesCallSqlTest {

  private Client client;
  private ArgumentCaptor<String> sqlCaptor;
  private QueryRequest defaultRequest;

  @BeforeEach
  void setUp() {
    client = mock(Client.class);

    GenericRecord traceIdRow = mock(GenericRecord.class);
    when(traceIdRow.getBigInteger("trace_id")).thenReturn(BigInteger.ONE);
    when(traceIdRow.getBigInteger("trace_id_high")).thenReturn(BigInteger.ZERO);

    when(client.queryAll(anyString(), anyMap(), any(QuerySettings.class)))
      .thenReturn(List.of(traceIdRow))
      .thenReturn(List.of());

    sqlCaptor = ArgumentCaptor.forClass(String.class);
    defaultRequest = QueryRequest.newBuilder()
      .endTs(System.currentTimeMillis())
      .lookback(60_000)
      .limit(10)
      .build();
  }

  private List<String> capturedSqls(GetTracesCall call) {
    call.doExecute();
    verify(client, times(2)).queryAll(sqlCaptor.capture(), anyMap(), any(QuerySettings.class));
    return sqlCaptor.getAllValues();
  }

  private String innerSql(GetTracesCall call) {
    return capturedSqls(call).get(0);
  }

  private String outerSql(GetTracesCall call) {
    return capturedSqls(call).get(1);
  }

  @Test
  void withStatistics_selectsStatsDurationColumns() {
    String sql = outerSql(new GetTracesCall(client, "zipkin", defaultRequest, 3, true));
    assertTrue(sql.contains("stats.median_duration"));
    assertTrue(sql.contains("stats.average_duration"));
    assertTrue(sql.contains("stats.p50"));
    assertTrue(sql.contains("stats.p95"));
    assertTrue(sql.contains("stats.p99"));
  }

  @Test
  void withStatistics_selectsStatsCountColumns() {
    String sql = outerSql(new GetTracesCall(client, "zipkin", defaultRequest, 3, true));
    assertTrue(sql.contains("stats.success_count"));
    assertTrue(sql.contains("stats.error_count"));
    assertTrue(sql.contains("stats.total_count"));
  }

  @Test
  void withStatistics_includesLeftJoinFragment() {
    String sql = outerSql(new GetTracesCall(client, "zipkin", defaultRequest, 3, true));
    assertTrue(sql.contains("LEFT JOIN"));
    assertTrue(sql.contains("spans_aggregate_stats"));
  }

  @Test
  void withStatistics_joinsOnNameKindServiceName() {
    String sql = outerSql(new GetTracesCall(client, "zipkin", defaultRequest, 3, true));
    assertTrue(sql.contains("s.name = stats.span_name"));
    assertTrue(sql.contains("s.kind = stats.span_kind"));
    assertTrue(sql.contains("s.local_endpoint_service_name = stats.service_name"));
  }

  @Test
  void withStatistics_usesCorrectDatabaseInJoin() {
    String sql = outerSql(new GetTracesCall(client, "analytics", defaultRequest, 3, true));
    assertTrue(sql.contains("analytics.spans_aggregate_stats"));
  }

  @Test
  void outerQuery_filtersByTraceIdInClause() {
    String sql = outerSql(new GetTracesCall(client, "zipkin", defaultRequest, 3, true));
    assertTrue(sql.contains("(s.trace_id, s.trace_id_high) IN ({traceIds:Array(Tuple(UInt64, UInt64))})"),
      "Outer query must restrict to the trace ids selected by the inner query");
  }

  @Test
  void innerQuery_selectsOnlyTraceIdColumns() {
    String sql = innerSql(new GetTracesCall(client, "zipkin", defaultRequest, 3, true));
    assertTrue(sql.contains("SELECT trace_id, trace_id_high FROM"));
    assertFalse(sql.contains("stats."), "Inner trace-id query must not reference the statistics join");
  }

  @Test
  void outerQuery_hasLimitBasedOnRequestAndMultiplier() {
    // limit=10, multiplier=3 → LIMIT 30
    String sql = outerSql(new GetTracesCall(client, "zipkin", defaultRequest, 3, false));
    assertTrue(sql.contains("LIMIT"), "Outer query must have LIMIT to prevent unbounded scans");
    assertTrue(sql.contains("30"));
  }

  @Test
  void innerQuery_usesFromUnixTimestampForPartitionPruning() {
    String sql = innerSql(new GetTracesCall(client, "zipkin", defaultRequest, 3, false));
    assertTrue(sql.contains("timestamp >= fromUnixTimestamp64Micro("), "Must use fromUnixTimestamp64Micro for partition pruning");
    assertTrue(sql.contains("timestamp <= fromUnixTimestamp64Micro("));
    assertFalse(sql.contains("toUnixTimestamp64Micro(timestamp)"), "Must not apply function to the column");
  }

  @Test
  void innerQuery_usesLimitNotGroupBy() {
    String sql = innerSql(new GetTracesCall(client, "zipkin", defaultRequest, 3, false));
    assertFalse(sql.contains("GROUP BY trace_id"), "GROUP BY forces full aggregation scan — use LIMIT instead");
    assertTrue(sql.contains("LIMIT 30"), "LIMIT bounds the trace-id selection scan");
  }

  @Test
  void withStatistics_withServiceName_statsJoinFiltersServiceName() {
    QueryRequest request = QueryRequest.newBuilder()
      .endTs(System.currentTimeMillis())
      .lookback(60_000)
      .limit(10)
      .serviceName("my-service")
      .build();
    String sql = outerSql(new GetTracesCall(client, "zipkin", request, 3, true));
    assertTrue(sql.contains("service_name = {statsServiceName:String}"),
      "Stats subquery must filter by service_name to avoid full table aggregation");
  }

  @Test
  void withStatistics_withoutServiceName_statsJoinHasNoWhereClause() {
    String sql = outerSql(new GetTracesCall(client, "zipkin", defaultRequest, 3, true));
    assertFalse(sql.contains("statsServiceName"), "Stats subquery must not add WHERE when no service filter");
  }

  @Test
  void withoutStatistics_doesNotSelectStatsColumns() {
    String sql = outerSql(new GetTracesCall(client, "zipkin", defaultRequest, 3, false));
    assertFalse(sql.contains("stats.median_duration"));
    assertFalse(sql.contains("stats.p50"));
  }

  @Test
  void withoutStatistics_doesNotIncludeLeftJoin() {
    String sql = outerSql(new GetTracesCall(client, "zipkin", defaultRequest, 3, false));
    assertFalse(sql.contains("LEFT JOIN"));
  }

  @Test
  void withServiceNameFilter_innerQueryHasServiceNameCondition() {
    QueryRequest request = QueryRequest.newBuilder()
      .endTs(System.currentTimeMillis())
      .lookback(60_000)
      .limit(10)
      .serviceName("my-service")
      .build();
    String sql = innerSql(new GetTracesCall(client, "zipkin", request, 3, true));
    assertTrue(sql.contains("local_endpoint_service_name = {serviceName:String}"));
  }

  @Test
  void withSpanNameFilter_innerQueryHasSpanNameCondition() {
    QueryRequest request = QueryRequest.newBuilder()
      .endTs(System.currentTimeMillis())
      .lookback(60_000)
      .limit(10)
      .serviceName("svc")
      .spanName("checkout")
      .build();
    String sql = innerSql(new GetTracesCall(client, "zipkin", request, 3, true));
    assertTrue(sql.contains("name = {spanName:String}"));
  }
}
