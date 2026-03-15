package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import zipkin2.Call;

import java.util.ArrayList;
import java.util.List;

public final class GetSpanNamesCall extends ClickHouseCall<List<String>> {
  private final String serviceName;

  GetSpanNamesCall(Client client, String database, String serviceName) {
    super(client, database);
    this.serviceName = serviceName;
  }

  @Override
  protected List<String> doExecute() {
    String sql = "SELECT DISTINCT operation_name FROM " + database + ".spans" +
      " WHERE service_name = '" + escape(serviceName) + "'" +
      " AND operation_name != ''" +
      " ORDER BY operation_name ASC";

    client.query(sql).executeAndWait();
    List<String> spanNames = new ArrayList<>();
    // TODO: Parse span names from result
    return spanNames;
  }

  @Override
  public Call<List<String>> clone() {
    return new GetSpanNamesCall(client, database, serviceName);
  }

  @Override
  public String toString() {
    return "GetSpanNamesCall{serviceName=" + serviceName + "}";
  }

  private static String escape(String value) {
    return value.replace("'", "\\'");
  }
}
