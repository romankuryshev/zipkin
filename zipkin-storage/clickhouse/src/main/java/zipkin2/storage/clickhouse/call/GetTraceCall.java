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

  public GetTraceCall(Client client, String database, String traceId) {
    super(client, database);
    this.traceId = Span.normalizeTraceId(traceId);
  }

  @Override
  protected List<Span> doExecute() {
    BigInteger[] traceParts = parseTraceId(traceId);
    String sql = "SELECT s.trace_id, s.span_id, s.name, s.kind, s.duration, s.status_code, " +
      "s.local_endpoint_service_name, s.local_endpoint_ipv4, s.local_endpoint_ipv6, s.local_endpoint_port, " +
      "s.remote_endpoint_service_name, s.remote_endpoint_ipv4, s.remote_endpoint_ipv6, s.remote_endpoint_port, " +
      "s.trace_id_high, s.parent_id, s.timestamp, s.tags, s.annotations, " +
      "stats.median_duration, stats.average_duration, stats.p50, stats.p95, stats.p99, " +
      "stats.success_count, stats.error_count, stats.total_count " +
      "FROM " + database + ".spans s" +
      ClickHouseResultMapper.getStatisticsJoinFragment(database) +
      " WHERE s.trace_id = {traceIdLow:UInt64}" +
      " AND s.trace_id_high = {traceIdHigh:UInt64}" +
      " ORDER BY s.timestamp ASC";

    java.util.Map<String, Object> queryParams = new java.util.HashMap<>();
    queryParams.put("traceIdLow", traceParts[0]);
    queryParams.put("traceIdHigh", traceParts[1]);

    try {
      QueryResponse response = client.query(sql, queryParams, new com.clickhouse.client.api.query.QuerySettings()).get();
      return ClickHouseResultMapper.toSpans(response, client);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Call<List<Span>> clone() {
    return new GetTraceCall(client, database, traceId);
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
