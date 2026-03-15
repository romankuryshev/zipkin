package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;

import java.util.ArrayList;
import java.util.List;

public final class GetTracesCall extends ClickHouseCall<List<List<Span>>> {
  private final QueryRequest request;

  GetTracesCall(Client client, String database, QueryRequest request) {
    super(client, database);
    this.request = request;
  }

  @Override
  protected List<List<Span>> doExecute() {
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT * FROM ").append(database).append(".spans")
      .append(" WHERE start_time >= toDateTime64(").append((request.endTs() - request.lookback()) * 1000).append(", 6)")
      .append(" AND start_time <= toDateTime64(").append(request.endTs() * 1000).append(", 6)");

    if (request.serviceName() != null) {
      sql.append(" AND service_name = '").append(escape(request.serviceName())).append("'");
    }

    if (request.spanName() != null) {
      sql.append(" AND operation_name = '").append(escape(request.spanName())).append("'");
    }

    sql.append(" ORDER BY start_time DESC")
      .append(" LIMIT ").append(request.limit() * 100);

    var result = client.query(sql.toString()).executeAndWait();
    List<List<Span>> traces = new ArrayList<>();
    // TODO: Parse result rows and group by trace_id
    return traces;
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
