package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import zipkin2.Call;

import java.util.ArrayList;
import java.util.List;

public final class GetServiceNamesCall extends ClickHouseCall<List<String>> {

  GetServiceNamesCall(Client client, String database) {
    super(client, database);
  }

  @Override
  protected List<String> doExecute() {
    String sql = "SELECT DISTINCT service_name FROM " + database + ".spans" +
      " WHERE service_name != ''" +
      " ORDER BY service_name ASC";

    client.query(sql).executeAndWait();
    List<String> serviceNames = new ArrayList<>();
    // TODO: Parse service names from result
    return serviceNames;
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
