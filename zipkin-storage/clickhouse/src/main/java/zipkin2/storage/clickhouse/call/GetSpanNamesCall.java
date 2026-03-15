package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import java.util.List;
import java.util.concurrent.ExecutionException;

public final class GetSpanNamesCall extends ClickHouseCall<List<String>> {
  private final String serviceName;

  public GetSpanNamesCall(Client client, String database, String serviceName) {
    super(client, database);
    this.serviceName = serviceName;
  }

  @Override
  protected List<String> doExecute() {
    String sql = "SELECT DISTINCT operation_name FROM " + database + ".spans" +
      " WHERE service_name = '" + escape(serviceName) + "'" +
      " AND operation_name != ''" +
      " ORDER BY operation_name ASC";

    QueryResponse response = null;
    try {
      response = client.query(sql).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    return ClickHouseResultMapper.toStringList(response, client, "operation_name");
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
