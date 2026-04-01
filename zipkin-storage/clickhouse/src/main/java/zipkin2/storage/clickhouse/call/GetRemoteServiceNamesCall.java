package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import java.util.List;
import java.util.concurrent.ExecutionException;

public final class GetRemoteServiceNamesCall extends ClickHouseCall<List<String>> {
  private final String serviceName;

  public GetRemoteServiceNamesCall(Client client, String database, String serviceName) {
    super(client, database);
    this.serviceName = serviceName;
  }

  @Override
  protected List<String> doExecute() {
    String sql = "SELECT DISTINCT remote_endpoint_service_name as service_name FROM " + database + ".spans" +
      " WHERE local_endpoint_service_name = {serviceName:String}" +
      " AND remote_endpoint_service_name != ''" +
      " ORDER BY service_name ASC";

    java.util.Map<String, Object> queryParams = new java.util.HashMap<>();
    queryParams.put("serviceName", serviceName);

    QueryResponse response = null;
    try {
      response = client.query(sql, queryParams, new com.clickhouse.client.api.query.QuerySettings()).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    return ClickHouseResultMapper.toStringList(response, client, "service_name");
  }

  @Override
  public Call<List<String>> clone() {
    return new GetRemoteServiceNamesCall(client, database, serviceName);
  }

  @Override
  public String toString() {
    return "GetRemoteServiceNamesCall{serviceName=" + serviceName + "}";
  }

  private static String escape(String value) {
    return value.replace("'", "\\'");
  }
}
