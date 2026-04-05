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

  public GetTracesCall(Client client, String database, QueryRequest request) {
    super(client, database);
    this.request = request;
  }

  @Override
  protected List<List<Span>> doExecute() {
    long endTs = request.endTs() / 1000L;
    long lookback = request.lookback() / 1000L;
    long startTime = endTs - lookback;

    StringBuilder sql = new StringBuilder();
    sql.append("SELECT s.trace_id, s.span_id, s.name, s.kind, s.duration, s.status_code, ")
      .append("s.local_endpoint_service_name, s.local_endpoint_ipv4, s.local_endpoint_ipv6, s.local_endpoint_port, ")
      .append("s.remote_endpoint_service_name, s.remote_endpoint_ipv4, s.remote_endpoint_ipv6, s.remote_endpoint_port, ")
      .append("s.trace_id_high, s.parent_id, s.timestamp, s.tags, s.annotations, ")
      .append("stats.median_duration, stats.average_duration, stats.p50, stats.p95, stats.p99, ")
      .append("stats.success_count, stats.error_count, stats.total_count ")
      .append("FROM ").append(database).append(".spans s")
      .append(ClickHouseResultMapper.getStatisticsJoinFragment(database))
      .append(" WHERE s.timestamp >= {startTime:DateTime64}")
      .append(" AND s.timestamp <= {endTime:DateTime64}");

    Map<String, Object> queryParams = new java.util.HashMap<>();
    queryParams.put("startTime", startTime);
    queryParams.put("endTime", endTs);

    if (request.serviceName() != null) {
      sql.append(" AND s.local_endpoint_service_name = {serviceName:String}");
      queryParams.put("serviceName", request.serviceName());
    }

    if (request.spanName() != null) {
      sql.append(" AND s.name = {spanName:String}");
      queryParams.put("spanName", request.spanName());
    }

    sql.append(" ORDER BY s.timestamp DESC")
      .append(" LIMIT ").append(request.limit());

    try {
      QueryResponse response = client.query(sql.toString(), queryParams, new com.clickhouse.client.api.query.QuerySettings()).get();
      List<Span> spans = ClickHouseResultMapper.toSpans(response, client);
      return ClickHouseResultMapper.groupSpansByTraceId(spans);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Call<List<List<Span>>> clone() {
    return new GetTracesCall(client, database, request);
  }

  @Override
  public String toString() {
    return "GetTracesCall{limit=" + request.limit() + "}";
  }
}
