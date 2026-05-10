package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public final class GetTracesCall extends ClickHouseCall<List<List<Span>>> {
  private final QueryRequest request;
  private final int maxSpansLimitMultiplier;
  private final boolean includeSpanStatistics;

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

    Map<String, Object> queryParams = new java.util.HashMap<>();
    queryParams.put("startTimeMicros", startTimeMicros);
    queryParams.put("endTimeMicros", endTsMicros);

    StringBuilder inner = new StringBuilder();
    inner.append("SELECT trace_id, trace_id_high FROM ").append(database).append(".spans")
      .append(" WHERE toUnixTimestamp64Micro(timestamp) >= {startTimeMicros:Int64}")
      .append(" AND toUnixTimestamp64Micro(timestamp) <= {endTimeMicros:Int64}");

    if (request.serviceName() != null) {
      inner.append(" AND local_endpoint_service_name = {serviceName:String}");
      queryParams.put("serviceName", request.serviceName().toLowerCase(java.util.Locale.ROOT));
    }

    if (request.spanName() != null) {
      inner.append(" AND name = {spanName:String}");
      queryParams.put("spanName", request.spanName());
    }

    if (request.remoteServiceName() != null) {
      inner.append(" AND remote_endpoint_service_name = {remoteServiceName:String}");
      queryParams.put("remoteServiceName", request.remoteServiceName());
    }

    if (request.minDuration() != null) {
      inner.append(" AND duration >= {minDuration:Int64}");
      queryParams.put("minDuration", request.minDuration());
    }

    if (request.maxDuration() != null) {
      inner.append(" AND duration <= {maxDuration:Int64}");
      queryParams.put("maxDuration", request.maxDuration());
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
          queryParams.put(ki, tagKey);
        } else {
          inner.append(" AND tags[{").append(ki).append(":String}] = {").append(vi).append(":String}");
          queryParams.put(ki, tagKey);
          queryParams.put(vi, tagValue);
        }
      }
    }

    inner.append(" GROUP BY trace_id, trace_id_high")
      .append(" ORDER BY max(timestamp) DESC")
      .append(" LIMIT ").append(request.limit());

    StringBuilder sql = new StringBuilder();
    sql.append("SELECT s.trace_id, s.span_id, s.name, s.kind, s.duration, s.status_code, ")
      .append("s.local_endpoint_service_name, s.local_endpoint_ipv4, s.local_endpoint_ipv6, s.local_endpoint_port, ")
      .append("s.remote_endpoint_service_name, s.remote_endpoint_ipv4, s.remote_endpoint_ipv6, s.remote_endpoint_port, ")
      .append("s.trace_id_high, s.parent_id, s.timestamp, s.tags, s.annotations, s.shared, s.debug");

    if (includeSpanStatistics) {
      sql.append(", stats.median_duration, stats.average_duration, stats.p50, stats.p95, stats.p99, ")
        .append("stats.success_count, stats.error_count, stats.total_count ");
    }

    sql.append(" FROM ").append(database).append(".spans s");

    if (includeSpanStatistics) {
      sql.append(ClickHouseResultMapper.getStatisticsJoinFragment(database));
    }

    sql.append(" WHERE (s.trace_id, s.trace_id_high) GLOBAL IN (").append(inner).append(")")
      .append(" ORDER BY s.timestamp DESC");

    try {
      QueryResponse response = client.query(sql.toString(), queryParams, newQuerySettings()).get();
      List<Span> spans = ClickHouseResultMapper.toSpans(response, client);
      return ClickHouseResultMapper.groupSpansByTraceId(spans);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Call<List<List<Span>>> clone() {
    return new GetTracesCall(client, database, request, maxSpansLimitMultiplier, includeSpanStatistics);
  }

  @Override
  public String toString() {
    return "GetTracesCall{limit=" + request.limit() + "}";
  }
}
