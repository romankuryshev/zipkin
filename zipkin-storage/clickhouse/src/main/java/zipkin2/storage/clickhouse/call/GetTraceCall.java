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
    long[] traceParts = parseTraceId(traceId);
    String sql = "SELECT * FROM " + database + ".spans" +
      " WHERE trace_id = {traceIdLow:UInt64}" +
      " AND trace_id_high = {traceIdHigh:UInt64}" +
      " ORDER BY timestamp ASC";

    java.util.Map<String, Object> queryParams = new java.util.HashMap<>();
    queryParams.put("traceIdLow", traceParts[0]);
    queryParams.put("traceIdHigh", traceParts[1]);

    QueryResponse response = null;
    try {
      response = client.query(sql, queryParams, new com.clickhouse.client.api.query.QuerySettings()).get();
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
