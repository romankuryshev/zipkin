package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import org.junit.jupiter.api.Test;
import zipkin2.Call;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ClickHouseAutocompleteTagsTest {

  @Test
  public void getKeysReturnsCallInstance() {
    var mockClient = mock(Client.class);
    var autocompleteTags = new ClickHouseAutocompleteTags(mockClient, "zipkin");

    Call<List<String>> result = autocompleteTags.getKeys();

    assertNotNull(result);
  }

  @Test
  public void getValuesReturnsCallInstance() {
    var mockClient = mock(Client.class);
    var autocompleteTags = new ClickHouseAutocompleteTags(mockClient, "zipkin");

    Call<List<String>> result = autocompleteTags.getValues("http.method");

    assertNotNull(result);
  }

  @Test
  public void getValuesWithDifferentKeys() {
    var mockClient = mock(Client.class);
    var autocompleteTags = new ClickHouseAutocompleteTags(mockClient, "zipkin");

    Call<List<String>> result1 = autocompleteTags.getValues("http.method");
    Call<List<String>> result2 = autocompleteTags.getValues("http.status_code");

    assertNotNull(result1);
    assertNotNull(result2);
    assertNotEquals(result1, result2);
  }

  @Test
  public void getValuesWithNullKeyThrowsException() {
    var mockClient = mock(Client.class);
    var autocompleteTags = new ClickHouseAutocompleteTags(mockClient, "zipkin");

    assertThrows(IllegalArgumentException.class, () -> autocompleteTags.getValues(null));
  }

  @Test
  public void getValuesWithEmptyKeyThrowsException() {
    var mockClient = mock(Client.class);
    var autocompleteTags = new ClickHouseAutocompleteTags(mockClient, "zipkin");

    assertThrows(IllegalArgumentException.class, () -> autocompleteTags.getValues(""));
  }

  @Test
  public void getValuesWithValidKey() {
    var mockClient = mock(Client.class);
    var autocompleteTags = new ClickHouseAutocompleteTags(mockClient, "zipkin");

    Call<List<String>> result = autocompleteTags.getValues("environment");

    assertNotNull(result);
  }

  @Test
  public void multipleCallsToGetKeys() {
    var mockClient = mock(Client.class);
    var autocompleteTags = new ClickHouseAutocompleteTags(mockClient, "zipkin");

    Call<List<String>> result1 = autocompleteTags.getKeys();
    Call<List<String>> result2 = autocompleteTags.getKeys();

    assertNotNull(result1);
    assertNotNull(result2);
    assertNotEquals(result1, result2);
  }

  @Test
  public void multipleCallsToGetValuesWithSameKey() {
    var mockClient = mock(Client.class);
    var autocompleteTags = new ClickHouseAutocompleteTags(mockClient, "zipkin");

    Call<List<String>> result1 = autocompleteTags.getValues("http.method");
    Call<List<String>> result2 = autocompleteTags.getValues("http.method");

    assertNotNull(result1);
    assertNotNull(result2);
    assertNotEquals(result1, result2);
  }

  @Test
  public void constructorWithDifferentDatabase() {
    var mockClient = mock(Client.class);
    var autocompleteTags = new ClickHouseAutocompleteTags(mockClient, "custom_db");

    assertNotNull(autocompleteTags);
  }

  @Test
  public void toStringContainsDatabaseName() {
    var mockClient = mock(Client.class);
    var autocompleteTags = new ClickHouseAutocompleteTags(mockClient, "zipkin");

    String result = autocompleteTags.toString();

    assertTrue(result.contains("zipkin"));
    assertTrue(result.contains("ClickHouseAutocompleteTags"));
  }

  @Test
  public void toStringWithDifferentDatabase() {
    var mockClient = mock(Client.class);
    var autocompleteTags = new ClickHouseAutocompleteTags(mockClient, "custom_db");

    String result = autocompleteTags.toString();

    assertTrue(result.contains("custom_db"));
  }

  @Test
  public void getValuesWithSpecialCharactersInKey() {
    var mockClient = mock(Client.class);
    var autocompleteTags = new ClickHouseAutocompleteTags(mockClient, "zipkin");

    Call<List<String>> result = autocompleteTags.getValues("http.status-code_v2");

    assertNotNull(result);
  }

  @Test
  public void getValuesWithLongKey() {
    var mockClient = mock(Client.class);
    var autocompleteTags = new ClickHouseAutocompleteTags(mockClient, "zipkin");

    String longKey = "a".repeat(255);
    Call<List<String>> result = autocompleteTags.getValues(longKey);

    assertNotNull(result);
  }

  @Test
  public void getValuesWithUnicodeKey() {
    var mockClient = mock(Client.class);
    var autocompleteTags = new ClickHouseAutocompleteTags(mockClient, "zipkin");

    Call<List<String>> result = autocompleteTags.getValues("тест.ключ");

    assertNotNull(result);
  }
}

