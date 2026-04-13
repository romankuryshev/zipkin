package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.clickhouse.cache.AutocompleteTagsCache;
import zipkin2.storage.clickhouse.call.GetAutocompleteKeysCall;
import zipkin2.storage.clickhouse.call.GetAutocompleteValuesCall;

import java.util.List;

public class ClickHouseAutocompleteTags implements AutocompleteTags {

  private final Client client;
  private final String database;
  private final AutocompleteTagsCache autocompleteTagsCache;

  public ClickHouseAutocompleteTags(Client client, String database) {
    this(client, database, null);
  }

  public ClickHouseAutocompleteTags(Client client, String database,
                                    AutocompleteTagsCache autocompleteTagsCache) {
    this.client = client;
    this.database = database;
    this.autocompleteTagsCache = autocompleteTagsCache;
  }

  @Override
  public Call<List<String>> getKeys() {
    return new GetAutocompleteKeysCall(client, database);
  }

  @Override
  public Call<List<String>> getValues(String key) {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Tag key cannot be empty");
    }
    return new GetAutocompleteValuesCall(client, database, key, autocompleteTagsCache);
  }

  @Override
  public String toString() {
    return "ClickHouseAutocompleteTags{" +
      "database='" + database + '\'' +
      ", cacheEnabled=" + (autocompleteTagsCache != null) +
      '}';
  }
}
