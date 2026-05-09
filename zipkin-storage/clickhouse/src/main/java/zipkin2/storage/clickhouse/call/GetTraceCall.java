package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import zipkin2.Span;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.ExecutionException;

public final class GetTraceCall extends ClickHouseCall<List<Span>> {
  private final String traceId;
  private final boolean includeSpanStatistics;

  public GetTraceCall(Client client, String database, String traceId) {
    this(client, database, traceId, true);
  }

  public GetTraceCall(Client client, String database, String traceId, boolean includeSpanStatistics) {
    super(client, database);
    this.traceId = Span.normalizeTraceId(traceId);
    this.includeSpanStatistics = includeSpanStatistics;
  }

  @Override
  protected List<Span> doExecute() {
    BigInteger[] traceParts = parseTraceId(traceId);

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

    sql.append(" WHERE s.trace_id = {traceIdLow:UInt64}")
      .append(" AND s.trace_id_high = {traceIdHigh:UInt64}")
      .append(" ORDER BY s.timestamp ASC");

    java.util.Map<String, Object> queryParams = new java.util.HashMap<>();
    queryParams.put("traceIdLow", traceParts[0]);
    queryParams.put("traceIdHigh", traceParts[1]);

    try {
      QueryResponse response = client.query(sql.toString(), queryParams, new com.clickhouse.client.api.query.QuerySettings()).get();
      return ClickHouseResultMapper.toSpans(response, client);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Call<List<Span>> clone() {
    return new GetTraceCall(client, database, traceId, includeSpanStatistics);
  }

  @Override
  public String toString() {
    return "GetTraceCall{traceId=" + traceId + "}";
  }

  private BigInteger[] parseTraceId(String hexTraceId) {
    if (hexTraceId == null || hexTraceId.isEmpty()) {
      return new BigInteger[]{BigInteger.ZERO, BigInteger.ZERO};
    }

    try {
      if (hexTraceId.length() <= 16) {
        BigInteger low = new BigInteger(hexTraceId, 16);
        return new BigInteger[]{low, BigInteger.ZERO};
      } else {
        String high = hexTraceId.substring(0, hexTraceId.length() - 16);
        String low = hexTraceId.substring(hexTraceId.length() - 16);
        BigInteger highBig = new BigInteger(high, 16);
        BigInteger lowBig = new BigInteger(low, 16);
        return new BigInteger[]{lowBig, highBig};
      }
    } catch (Exception e) {
      return new BigInteger[]{BigInteger.ZERO, BigInteger.ZERO};
    }
  }
}
