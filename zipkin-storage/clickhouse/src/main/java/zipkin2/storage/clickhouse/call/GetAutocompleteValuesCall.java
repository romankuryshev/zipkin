package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import java.util.List;
import java.util.concurrent.ExecutionException;

public final class GetAutocompleteValuesCall extends ClickHouseCall<List<String>> {
  private final String tagKey;

  public GetAutocompleteValuesCall(Client client, String database, String tagKey) {
    super(client, database);
    this.tagKey = tagKey;
  }

  @Override
  protected List<String> doExecute() {
    if (tagKey == null || tagKey.isEmpty()) {
      throw new IllegalArgumentException("Tag key cannot be empty");
    }

    String sql = "SELECT DISTINCT tags[{tagKey:String}] as value FROM " + database + ".spans " +
      "WHERE tags[{tagKey:String}] != '' " +
      "ORDER BY value ASC";

    java.util.Map<String, Object> queryParams = new java.util.HashMap<>();
    queryParams.put("tagKey", tagKey);

    QueryResponse response = null;
    try {
      response = client.query(sql, queryParams, new com.clickhouse.client.api.query.QuerySettings()).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    return ClickHouseResultMapper.toStringList(response, client, "value");
  }

  @Override
  public Call<List<String>> clone() {
    return new GetAutocompleteValuesCall(client, database, tagKey);
  }

  @Override
  public String toString() {
    return "GetAutocompleteValuesCall{tagKey=" + tagKey + "}";
  }

  private static String escape(String value) {
    return value.replace("'", "\\'");
  }
}
