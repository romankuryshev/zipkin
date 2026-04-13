package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.Call;
import zipkin2.storage.clickhouse.cache.AutocompleteTagsCache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public final class GetAutocompleteValuesCall extends ClickHouseCall<List<String>> {
  private final String tagKey;
  private final AutocompleteTagsCache autocompleteTagsCache;

  public GetAutocompleteValuesCall(Client client, String database, String tagKey,
                                   AutocompleteTagsCache autocompleteTagsCache) {
    super(client, database);
    this.tagKey = tagKey;
    this.autocompleteTagsCache = autocompleteTagsCache;
  }

  @Override
  protected List<String> doExecute() {
    if (tagKey == null || tagKey.isEmpty()) {
      throw new IllegalArgumentException("Tag key cannot be empty");
    }

    if (autocompleteTagsCache == null) {
      return Collections.emptyList();
    }
    Set<String> cachedValues = autocompleteTagsCache.get(tagKey);
    if (!cachedValues.isEmpty()) {
      return new ArrayList<>(cachedValues);
    }
    List<String> values = queryDatabase();
    autocompleteTagsCache.put(tagKey, values);
    return values;
  }

  private List<String> queryDatabase() {
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
    return new GetAutocompleteValuesCall(client, database, tagKey, autocompleteTagsCache);
  }

  @Override
  public String toString() {
    return "GetAutocompleteValuesCall{tagKey=" + tagKey + "}";
  }
}
