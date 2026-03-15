package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.Span;

import java.util.ArrayList;
import java.util.List;

public final class GetTraceCall extends ClickHouseCall<List<Span>> {
  private final String traceId;

  GetTraceCall(Client client, String database, String traceId) {
    super(client, database);
    this.traceId = Span.normalizeTraceId(traceId);
  }

  @Override
  protected List<Span> doExecute() {
    String sql = "SELECT * FROM " + database + ".spans" +
      " WHERE trace_id = '" + escape(traceId) + "'" +
      " ORDER BY start_time ASC";

    client.query(sql).executeAndWait();
    List<Span> spans = new ArrayList<>();
    // TODO: Parse result rows to Span objects
    return spans;
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
