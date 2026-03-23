package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.clickhouse.call.GetAutocompleteKeysCall;
import zipkin2.storage.clickhouse.call.GetAutocompleteValuesCall;

import java.util.List;

/**
 * ClickHouse implementation of AutocompleteTags interface.
 * Provides autocomplete functionality for tag keys and values.
 */
public class ClickHouseAutocompleteTags implements AutocompleteTags {

  private final Client client;
  private final String database;

  public ClickHouseAutocompleteTags(Client client, String database) {
    this.client = client;
    this.database = database;
  }

  /**
   * Retrieves the list of tag keys whose values may be returned by getValues(String).
   * These are usually configured via StorageComponent.Builder#autocompleteKeys(List).
   *
   * @return Call that returns a sorted list of tag keys
   */
  @Override
  public Call<List<String>> getKeys() {
    return new GetAutocompleteKeysCall(client, database);
  }

  /**
   * Retrieves the list of values for a given tag key.
   * If a key is not configured or there are no values available, an empty result will be returned.
   *
   * @param key the tag key
   * @return Call that returns a sorted list of values for the tag key
   * @throws IllegalArgumentException if the input is empty
   */
  @Override
  public Call<List<String>> getValues(String key) {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Tag key cannot be empty");
    }
    return new GetAutocompleteValuesCall(client, database, key);
  }

  @Override
  public String toString() {
    return "ClickHouseAutocompleteTags{" +
      "database='" + database + '\'' +
      '}';
  }
}
