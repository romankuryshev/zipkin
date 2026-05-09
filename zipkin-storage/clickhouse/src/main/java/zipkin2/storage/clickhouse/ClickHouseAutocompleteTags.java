package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.clickhouse.cache.AutocompleteTagsCache;
import zipkin2.storage.clickhouse.call.GetAutocompleteValuesCall;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ClickHouseAutocompleteTags implements AutocompleteTags {

  private final Client client;
  private final String database;
  private final List<String> autocompleteKeys;
  private final AutocompleteTagsCache autocompleteTagsCache;

  public ClickHouseAutocompleteTags(Client client, String database, Set<String> autocompleteKeys,
                                    AutocompleteTagsCache autocompleteTagsCache) {
    this.client = client;
    this.database = database;
    List<String> sorted = new ArrayList<>(autocompleteKeys);
    Collections.sort(sorted);
    this.autocompleteKeys = Collections.unmodifiableList(sorted);
    this.autocompleteTagsCache = autocompleteTagsCache;
  }

  @Override
  public Call<List<String>> getKeys() {
    return Call.create(autocompleteKeys);
  }

  @Override
  public Call<List<String>> getValues(String key) {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Tag key cannot be empty");
    }
    if (!autocompleteKeys.contains(key)) {
      return Call.create(Collections.emptyList());
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
