package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import zipkin2.Span;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class GetTracesByIdCall extends ClickHouseCall<List<List<Span>>> {

  private final Iterable<String> traceIds;

  public GetTracesByIdCall(Client client, String database, Iterable<String> traceIds) {
    super(client, database);
    this.traceIds = traceIds;
  }

  @Override
  protected List<List<Span>> doExecute() {
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT s.trace_id, s.span_id, s.name, s.kind, s.duration, s.status_code, ")
      .append("s.local_endpoint_service_name, s.local_endpoint_ipv4, s.local_endpoint_ipv6, s.local_endpoint_port, ")
      .append("s.remote_endpoint_service_name, s.remote_endpoint_ipv4, s.remote_endpoint_ipv6, s.remote_endpoint_port, ")
      .append("s.trace_id_high, s.parent_id, s.timestamp, s.tags, s.annotations, ")
      .append("stats.median_duration, stats.average_duration, stats.p50, stats.p95, stats.p99, ")
      .append("stats.success_count, stats.error_count, stats.total_count ")
      .append("FROM ").append(database).append(".spans s")
      .append(ClickHouseResultMapper.getStatisticsJoinFragment(database))
      .append(" WHERE s.trace_id IN {trace_ids_list:Array(UInt64)}")
      .append(" ORDER BY s.timestamp DESC");

    List<Long> traceIdsList = new ArrayList<>();
    var it = traceIds.iterator();
    traceIdsList.add(traceId(it.next()));
    while (it.hasNext()) {
      traceIdsList.add(traceId(it.next()));
    }
    java.util.Map<String, Object> queryParams = new java.util.HashMap<>();
    queryParams.put("trace_ids_list", traceIdsList);
    try {
      QueryResponse response = client.query(sql.toString(), queryParams, new com.clickhouse.client.api.query.QuerySettings()).get();
      List<Span> spans = ClickHouseResultMapper.toSpans(response, client);
      return ClickHouseResultMapper.groupSpansByTraceId(spans);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public long traceId(String traceId) {
    return parseTraceId(traceId)[0];
  }

  @Override
  public Call<List<List<Span>>> clone() {
    return new GetTracesByIdCall(client, database, traceIds);
  }

  @Override
  public String toString() {
    return "GetTracesByIdCall{traceIds=" + traceIds + "}";
  }

  private long[] parseTraceId(String hexTraceId) {
    if (hexTraceId == null || hexTraceId.isEmpty()) {
      return new long[]{0L, 0L};
    }

    try {
      if (hexTraceId.length() <= 16) {
        return new long[]{Long.parseUnsignedLong(hexTraceId, 16), 0L};
      } else {
        String high = hexTraceId.substring(0, hexTraceId.length() - 16);
        String low = hexTraceId.substring(hexTraceId.length() - 16);
        return new long[]{Long.parseUnsignedLong(low, 16), Long.parseUnsignedLong(high, 16)};
      }
    } catch (Exception e) {
      return new long[]{0L, 0L};
    }
  }
}
