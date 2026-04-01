package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public final class GetAutocompleteKeysCall extends ClickHouseCall<List<String>> {

  public GetAutocompleteKeysCall(Client client, String database) {
    super(client, database);
  }

  @Override
  protected List<String> doExecute() {
    String sql = "SELECT mapKeys(tags) as keys FROM " + database + ".spans " +
      "WHERE tags != {} ARRAY JOIN keys as key " +
      "GROUP BY key ORDER BY key";

    QueryResponse response = null;
    try {
      response = client.query(sql, new java.util.HashMap<>(), new com.clickhouse.client.api.query.QuerySettings()).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    return extractKeys(response);
  }

  private List<String> extractKeys(QueryResponse response) {
    Set<String> keys = new HashSet<>();

    try (ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
      while (reader.hasNext()) {
        Map<String, Object> record = reader.next();
        String key = (String) record.get("key");
        if (key != null && !key.isEmpty()) {
          keys.add(key);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read autocomplete keys from ClickHouse", e);
    }

    return keys.stream()
      .sorted()
      .collect(Collectors.toList());
  }

  @Override
  public Call<List<String>> clone() {
    return new GetAutocompleteKeysCall(client, database);
  }

  @Override
  public String toString() {
    return "GetAutocompleteKeysCall{}";
  }
}
