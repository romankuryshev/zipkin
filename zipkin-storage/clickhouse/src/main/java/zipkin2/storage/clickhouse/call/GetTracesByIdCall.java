package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import zipkin2.Span;

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
    sql.append("SELECT * FROM ").append(database).append(".spans")
      .append(" WHERE (trace_id, trace_id_high) IN (");

    var it = traceIds.iterator();
    while (it.hasNext()) {
      String traceId = Span.normalizeTraceId(it.next());
      long[] traceParts = parseTraceId(traceId);
      sql.append("(").append(traceParts[0]).append(",").append(traceParts[1]).append(")");
      if (it.hasNext()) {
        sql.append(",");
      }
    }
    sql.append(") ORDER BY timestamp DESC");

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

  private static String escape(String value) {
    return value.replace("'", "\\'");
  }
}
