package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;

import java.util.ArrayList;
import java.util.List;

public final class GetDependenciesCall extends ClickHouseCall<List<DependencyLink>> {
  private final long endTs;
  private final long lookback;

  GetDependenciesCall(Client client, String database, long endTs, long lookback) {
    super(client, database);
    this.endTs = endTs;
    this.lookback = lookback;
  }

  @Override
  protected List<DependencyLink> doExecute() {
    String sql = "SELECT * FROM " + database + ".spans" +
      " WHERE start_time >= toDateTime64(" + (endTs - lookback) * 1000 + ", 6)" +
      " AND start_time <= toDateTime64(" + endTs * 1000 + ", 6)";

    var queryResponse = client.query(sql).get();
    List<List<Span>> traces = new ArrayList<>();
    // TODO: Parse traces from result

    return DependencyLinker.merge(traces);
  }

  @Override
  public Call<List<DependencyLink>> clone() {
    return new GetDependenciesCall(client, database, endTs, lookback);
  }

  @Override
  public String toString() {
    return "GetDependenciesCall{endTs=" + endTs + ", lookback=" + lookback + "}";
  }
}
