package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import java.util.List;
import java.util.concurrent.ExecutionException;

public final class GetDependenciesCall extends ClickHouseCall<List<DependencyLink>> {
  private final long endTs;
  private final long lookback;

  public GetDependenciesCall(Client client, String database, long endTs, long lookback) {
    super(client, database);
    this.endTs = endTs;
    this.lookback = lookback;
  }

  @Override
  protected List<DependencyLink> doExecute() {
    String sql = "SELECT * FROM " + database + ".spans" +
      " WHERE start_time >= " + (endTs - lookback) +
      " AND start_time <= " + endTs;

    QueryResponse response = null;
    try {
      response = client.query(sql).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    List<Span> spans = ClickHouseResultMapper.toSpans(response, client);
    return ClickHouseResultMapper.extractDependencies(spans);
  }

  @Override
  public Call<List<DependencyLink>> clone() {
    return new GetDependenciesCall(client, database, endTs, lookback);
  }

  @Override
  public String toString() {
    return "GetDependenciesCall{endTs=" + endTs + ",lookback=" + lookback + "}";
  }
}
