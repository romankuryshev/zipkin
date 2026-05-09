package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import zipkin2.DependencyLink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    long endTsMicros = endTs * 1000L;
    long startTsMicros = endTsMicros - lookback * 1000L;

    String sql = "SELECT local_service_name, remote_service_name, "
      + "sum(call_count) AS call_count, sum(error_count) AS error_count "
      + "FROM " + database + ".dependencies "
      + "WHERE toUnixTimestamp64Micro(timestamp) >= {startTsMicros:Int64} "
      + "AND toUnixTimestamp64Micro(timestamp) <= {endTsMicros:Int64} "
      + "GROUP BY local_service_name, remote_service_name "
      + "HAVING call_count > 0";

    Map<String, Object> params = new HashMap<>();
    params.put("startTsMicros", startTsMicros);
    params.put("endTsMicros", endTsMicros);

    try {
      QueryResponse response = client.query(sql, params, newQuerySettings()).get();

      List<DependencyLink> links = new ArrayList<>();
      try (ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
        while (reader.hasNext()) {
          Map<String, Object> record = reader.next();
          String parent = (String) record.get("local_service_name");
          String child = (String) record.get("remote_service_name");
          if (parent == null || parent.isEmpty() || child == null || child.isEmpty()) continue;

          Object callCountObj = record.get("call_count");
          Object errorCountObj = record.get("error_count");
          long callCount = callCountObj instanceof Number ? ((Number) callCountObj).longValue() : 1L;
          long errorCount = errorCountObj instanceof Number ? ((Number) errorCountObj).longValue() : 0L;

          links.add(DependencyLink.newBuilder()
            .parent(parent)
            .child(child)
            .callCount(callCount)
            .errorCount(errorCount)
            .build());
        }
      }
      return links;
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read dependencies from ClickHouse", e);
    }
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
