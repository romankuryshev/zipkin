package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import java.util.List;
import java.util.concurrent.ExecutionException;

public final class GetServiceNamesCall extends ClickHouseCall<List<String>> {

  public GetServiceNamesCall(Client client, String database) {
    super(client, database);
  }

  @Override
  protected List<String> doExecute() {
    String sql = "SELECT DISTINCT service_name FROM " + database + ".spans" +
      " WHERE service_name != ''" +
      " ORDER BY service_name ASC";

    QueryResponse response = null;
    try {
      response = client.query(sql).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    return ClickHouseResultMapper.toStringList(response, client, "service_name");
  }

  @Override
  public Call<List<String>> clone() {
    return new GetServiceNamesCall(client, database);
  }

  @Override
  public String toString() {
    return "GetServiceNamesCall{}";
  }
}
