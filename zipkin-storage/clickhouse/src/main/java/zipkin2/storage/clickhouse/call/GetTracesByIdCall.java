package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import zipkin2.Span;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class GetTracesByIdCall extends ClickHouseCall<List<List<Span>>> {

  private final Iterable<String> traceIds;
  private final boolean includeSpanStatistics;

  public GetTracesByIdCall(Client client, String database, Iterable<String> traceIds) {
    this(client, database, traceIds, false);
  }

  public GetTracesByIdCall(Client client, String database, Iterable<String> traceIds,
                           boolean includeSpanStatistics) {
    super(client, database);
    this.traceIds = traceIds;
    this.includeSpanStatistics = includeSpanStatistics;
  }

  @Override
  protected List<List<Span>> doExecute() {
    List<BigInteger[]> parsedIds = new ArrayList<>();
    for (String id : traceIds) {
      parsedIds.add(parseTraceId(id));
    }

    if (parsedIds.isEmpty()) {
      return List.of();
    }

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
      sql.append(ClickHouseResultMapper.getStatisticsJoinFragment(database, null));
    }

    Map<String, Object> queryParams = new java.util.HashMap<>();
    sql.append(" WHERE ");
    for (int i = 0; i < parsedIds.size(); i++) {
      if (i > 0) sql.append(" OR ");
      sql.append("(s.trace_id = {low").append(i).append(":UInt64}")
        .append(" AND s.trace_id_high = {high").append(i).append(":UInt64})");
      queryParams.put("low" + i, parsedIds.get(i)[0]);
      queryParams.put("high" + i, parsedIds.get(i)[1]);
    }

    sql.append(" ORDER BY s.timestamp DESC");

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
    return new GetTracesByIdCall(client, database, traceIds, includeSpanStatistics);
  }

  @Override
  public String toString() {
    return "GetTracesByIdCall{traceIds=" + traceIds + "}";
  }

  private BigInteger[] parseTraceId(String hexTraceId) {
    if (hexTraceId == null || hexTraceId.isEmpty()) {
      return new BigInteger[]{BigInteger.ZERO, BigInteger.ZERO};
    }
    try {
      if (hexTraceId.length() <= 16) {
        return new BigInteger[]{new BigInteger(hexTraceId, 16), BigInteger.ZERO};
      } else {
        String high = hexTraceId.substring(0, hexTraceId.length() - 16);
        String low = hexTraceId.substring(hexTraceId.length() - 16);
        return new BigInteger[]{new BigInteger(low, 16), new BigInteger(high, 16)};
      }
    } catch (Exception e) {
      return new BigInteger[]{BigInteger.ZERO, BigInteger.ZERO};
    }
  }
}
