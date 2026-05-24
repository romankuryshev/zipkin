package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class GetTracesCall extends ClickHouseCall<List<List<Span>>> {
  private final QueryRequest request;
  private final int maxSpansLimitMultiplier;
  private final boolean includeSpanStatistics;
  private static final int multIndex = 3;

  public GetTracesCall(Client client, String database, QueryRequest request,
                       int maxSpansLimitMultiplier, boolean includeSpanStatistics) {
    super(client, database);
    this.request = request;
    this.maxSpansLimitMultiplier = maxSpansLimitMultiplier;
    this.includeSpanStatistics = includeSpanStatistics;
  }

  @Override
  protected List<List<Span>> doExecute() {
    long endTsMicros = request.endTs() * 1000L;
    long startTimeMicros = endTsMicros - request.lookback() * 1000L;

    Map<String, Object> innerParams = new java.util.HashMap<>();
    innerParams.put("startTimeMicros", startTimeMicros);
    innerParams.put("endTimeMicros", endTsMicros);

    StringBuilder inner = new StringBuilder();
    inner.append("SELECT trace_id, trace_id_high FROM ").append(database).append("""
      .spans
      WHERE timestamp >= fromUnixTimestamp64Micro({startTimeMicros:Int64})
      AND timestamp <= fromUnixTimestamp64Micro({endTimeMicros:Int64})
      """);

    if (request.serviceName() != null) {
      inner.append(" AND local_endpoint_service_name = {serviceName:String}");
      innerParams.put("serviceName", request.serviceName().toLowerCase(java.util.Locale.ROOT));
    }

    if (request.spanName() != null) {
      inner.append(" AND name = {spanName:String}");
      innerParams.put("spanName", request.spanName());
    }

    if (request.remoteServiceName() != null) {
      inner.append(" AND remote_endpoint_service_name = {remoteServiceName:String}");
      innerParams.put("remoteServiceName", request.remoteServiceName());
    }

    if (request.minDuration() != null) {
      inner.append(" AND duration >= {minDuration:Int64}");
      innerParams.put("minDuration", request.minDuration());
    }

    if (request.maxDuration() != null) {
      inner.append(" AND duration <= {maxDuration:Int64}");
      innerParams.put("maxDuration", request.maxDuration());
    }

    if (request.annotationQuery() != null && !request.annotationQuery().isEmpty()) {
      int idx = 0;
      for (Map.Entry<String, String> entry : request.annotationQuery().entrySet()) {
        String tagKey = entry.getKey();
        String tagValue = entry.getValue();
        String ki = "aqk" + idx;
        String vi = "aqv" + idx;
        idx++;
        if (tagValue.isEmpty()) {
          inner.append(" AND (arrayExists(x -> x.2 = {").append(ki).append(":String}, annotations)")
            .append(" OR mapContains(tags, {").append(ki).append(":String}))");
          innerParams.put(ki, tagKey);
        } else {
          inner.append(" AND tags[{").append(ki).append(":String}] = {").append(vi).append(":String}");
          innerParams.put(ki, tagKey);
          innerParams.put(vi, tagValue);
        }
      }
    }
    inner.append(" LIMIT ").append(request.limit() * multIndex)
      .append(" SETTINGS max_threads = 2;");
    List<GenericRecord> traceIdsRows = client.queryAll(inner.toString(), innerParams, newQuerySettings());
    if (traceIdsRows.isEmpty()) {
      return List.of();
    }

    Set<TraceKey> traceIds = traceIdsRows.stream()
      .map(row -> new TraceKey(row.getBigInteger("trace_id"), row.getBigInteger("trace_id_high")))
      .collect(Collectors.toSet());

    Map<String, Object> sqlParams = new HashMap<>();
    sqlParams.put("traceIds", traceIds);
    sqlParams.put("startTimeMicros", startTimeMicros);
    sqlParams.put("endTimeMicros", endTsMicros);
    StringBuilder sql = new StringBuilder("""
      SELECT s.trace_id, s.span_id, s.name, s.kind, s.duration, s.status_code,
      s.local_endpoint_service_name, s.local_endpoint_ipv4, s.local_endpoint_ipv6, s.local_endpoint_port,
      s.remote_endpoint_service_name, s.remote_endpoint_ipv4, s.remote_endpoint_ipv6, s.remote_endpoint_port,
      s.trace_id_high, s.parent_id, s.timestamp, s.tags, s.annotations, s.shared, s.debug
      """);

    if (includeSpanStatistics) {
      sql.append("""
        , stats.median_duration AS median_duration
        , stats.average_duration AS average_duration
        , stats.p50 AS p50
        , stats.p95 AS p95
        , stats.p99 AS p99
        , stats.success_count AS success_count
        , stats.error_count AS error_count
        , stats.total_count AS total_count
      """);
    }

    sql.append(" FROM ").append(database).append(".spans s ");

    if (includeSpanStatistics) {
      sql.append(ClickHouseResultMapper.getStatisticsJoinFragment(database, request.serviceName()));
      if (request.serviceName() != null) {
        sqlParams.put("statsServiceName", request.serviceName().toLowerCase(java.util.Locale.ROOT));
      }
    }

    sql.append("""
      WHERE (s.trace_id, s.trace_id_high) IN ({traceIds:Array(Tuple(UInt64, UInt64))})
      AND timestamp >= fromUnixTimestamp64Micro({startTimeMicros:Int64})
      AND timestamp <= fromUnixTimestamp64Micro({endTimeMicros:Int64})
      LIMIT
      """)
      .append((long) request.limit() * maxSpansLimitMultiplier)
      .append(" SETTINGS max_threads = 2;");

    List<GenericRecord> rows = client.queryAll(sql.toString(), sqlParams, newQuerySettings());
    List<Span> spans = ClickHouseResultMapper.toSpans(rows, includeSpanStatistics);
    return ClickHouseResultMapper.groupSpansByTraceId(spans);
  }

  @Override
  public Call<List<List<Span>>> clone() {
    return new GetTracesCall(client, database, request, maxSpansLimitMultiplier, includeSpanStatistics);
  }

  @Override
  public String toString() {
    return "GetTracesCall{limit=" + request.limit() + "}";
  }

  record TraceKey(
    BigInteger traceId,
    BigInteger traceIdHigh
  ) {
    @Override
    public String toString() {
      return "(" + traceId + ", " + traceIdHigh + ")";
    }
  }
}
