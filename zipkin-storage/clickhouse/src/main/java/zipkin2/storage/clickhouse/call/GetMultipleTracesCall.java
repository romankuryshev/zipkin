package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.Span;

import java.util.ArrayList;
import java.util.List;

public final class GetMultipleTracesCall extends ClickHouseCall<List<List<Span>>> {
  private final Iterable<String> traceIds;

  GetMultipleTracesCall(Client client, String database, Iterable<String> traceIds) {
    super(client, database);
    this.traceIds = traceIds;
  }

  @Override
  protected List<List<Span>> doExecute() {
    StringBuilder traceIdList = new StringBuilder();
    int count = 0;
    for (String traceId : traceIds) {
      if (count > 0) traceIdList.append(",");
      traceIdList.append("'").append(escape(Span.normalizeTraceId(traceId))).append("'");
      count++;
    }

    if (count == 0) return new ArrayList<>();

    String sql = "SELECT * FROM " + database + ".spans" +
      " WHERE trace_id IN (" + traceIdList + ")" +
      " ORDER BY trace_id, start_time ASC";

    client.query(sql).executeAndWait();
    List<List<Span>> traces = new ArrayList<>();
    // TODO: Parse and group by trace_id
    return traces;
  }

  @Override
  public Call<List<List<Span>>> clone() {
    return new GetMultipleTracesCall(client, database, traceIds);
  }

  @Override
  public String toString() {
    return "GetMultipleTracesCall{}";
  }

  private static String escape(String value) {
    return value.replace("'", "\\'");
  }
}
