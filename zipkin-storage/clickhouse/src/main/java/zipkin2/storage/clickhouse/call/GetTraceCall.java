package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import zipkin2.Span;
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
    String sql = "SELECT * FROM " + database + ".spans" +
      " WHERE trace_id = '" + escape(traceId) + "'" +
      " ORDER BY start_time ASC";


    QueryResponse response = null;
    try {
      response = client.query(sql).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    return ClickHouseResultMapper.toSpans(response, client);
  }

  @Override
  public Call<List<Span>> clone() {
    return new GetTraceCall(client, database, traceId);
  }

  @Override
  public String toString() {
    return "GetTraceCall{traceId=" + traceId + "}";
  }

  private static String escape(String value) {
    return value.replace("'", "\\'");
  }
}
