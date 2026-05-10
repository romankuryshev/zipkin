package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.Traces;
import zipkin2.storage.clickhouse.call.GetTraceCall;
import zipkin2.storage.clickhouse.call.GetTracesByIdCall;

import java.util.List;

public class ClickHouseTraces implements Traces {

  private final Client client;
  private final String database;
  private final boolean includeSpanStatistics;

  public ClickHouseTraces(Client client, String database, boolean includeSpanStatistics) {
    this.client = client;
    this.database = database;
    this.includeSpanStatistics = includeSpanStatistics;
  }

  @Override
  public Call<List<Span>> getTrace(String traceId) {
    return new GetTraceCall(client, database, traceId, includeSpanStatistics);
  }

  @Override
  public Call<List<List<Span>>> getTraces(Iterable<String> traceIds) {
    return new GetTracesByIdCall(client, database, traceIds, includeSpanStatistics);
  }

  @Override
  public String toString() {
    return "ClickHouseTraces{" +
      "database='" + database + '\'' +
      '}';
  }
}
