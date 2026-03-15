package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;
import zipkin2.storage.clickhouse.call.*;

import java.util.List;

public class ClickHouseSpanStore implements SpanStore {

  private final Client client;
  private final String database;

  public ClickHouseSpanStore(Client client, String database) {
    this.client = client;
    this.database = database;
  }

  @Override
  public Call<List<List<Span>>> getTraces(QueryRequest request) {
    return new GetTracesCall(client, database, request);
  }

  @Override
  public Call<List<Span>> getTrace(String traceId) {
    return new GetTraceCall(client, database, traceId);
  }

  @Override
  public Call<List<String>> getServiceNames() {
    return new GetServiceNamesCall(client, database);
  }

  @Override
  public Call<List<String>> getSpanNames(String serviceName) {
    return new GetSpanNamesCall(client, database, serviceName);
  }

  @Override
  public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    return new GetDependenciesCall(client, database, endTs, lookback);
  }
}
