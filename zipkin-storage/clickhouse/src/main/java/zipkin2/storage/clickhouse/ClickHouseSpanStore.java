package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStatistics;
import zipkin2.storage.SpanStore;
import zipkin2.storage.clickhouse.call.*;

import java.util.List;

public class ClickHouseSpanStore implements SpanStore {

  private final Client client;
  private final String database;
  private final boolean strictTraceId;
  private final int maxSpansLimitMultiplier;

  public ClickHouseSpanStore(Client client, String database, boolean strictTraceId) {
    this(client, database, strictTraceId, 100);
  }

  public ClickHouseSpanStore(Client client, String database, boolean strictTraceId,
                             int maxSpansLimitMultiplier) {
    this.client = client;
    this.database = database;
    this.strictTraceId = strictTraceId;
    this.maxSpansLimitMultiplier = maxSpansLimitMultiplier;
  }

  @Override
  public Call<List<List<Span>>> getTraces(QueryRequest request) {
    return new GetTracesCall(client, database, request, maxSpansLimitMultiplier);
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

  @Override
  public Call<SpanStatistics> getSpanStatistics(
    String serviceName,
    String spanName,
    String spanKind,
    long endTs,
    long lookback) {
    return new GetSpanStatisticsCall(client, database, serviceName, spanName, spanKind, endTs, lookback);
  }

  public boolean isStrictTraceId() {
    return strictTraceId;
  }
}

