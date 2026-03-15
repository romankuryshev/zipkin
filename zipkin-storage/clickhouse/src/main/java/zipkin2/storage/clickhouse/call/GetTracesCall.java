package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import java.util.List;
import java.util.concurrent.ExecutionException;

public final class GetTracesCall extends ClickHouseCall<List<List<Span>>> {
  private final QueryRequest request;

  public GetTracesCall(Client client, String database, QueryRequest request) {
    super(client, database);
    this.request = request;
  }

  @Override
  protected List<List<Span>> doExecute() {
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT * FROM ").append(database).append(".spans")
      .append(" WHERE start_time >= ").append((request.endTs() - request.lookback()))
      .append(" AND start_time <= ").append(request.endTs());

    if (request.serviceName() != null) {
      sql.append(" AND service_name = '").append(escape(request.serviceName())).append("'");
    }

    if (request.spanName() != null) {
      sql.append(" AND operation_name = '").append(escape(request.spanName())).append("'");
    }

    sql.append(" ORDER BY start_time DESC")
      .append(" LIMIT ").append(request.limit() * 100);

    QueryResponse response = null;
    try {
      response = client.query(sql.toString()).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    List<Span> spans = ClickHouseResultMapper.toSpans(response, client);
    return ClickHouseResultMapper.groupSpansByTraceId(spans);
  }

  @Override
  public Call<List<List<Span>>> clone() {
    return new GetTracesCall(client, database, request);
  }

  @Override
  public String toString() {
    return "GetTracesCall{limit=" + request.limit() + "}";
  }

  private static String escape(String value) {
    return value.replace("'", "\\'");
  }
}
