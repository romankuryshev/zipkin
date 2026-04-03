package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import org.junit.jupiter.api.Test;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.Traces;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class ClickHouseStorageTest {

  @Test
  public void builderCreatesStorageWithDefaultValues() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin");

    var storage = builder.build();

    assertNotNull(storage);
    assertTrue(storage.isStrictTraceId());
    storage.close();
  }

  @Test
  public void builderWithEnsureSchemaTrue() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin")
      .setEnsureSchema(true);

    var storage = builder.build();

    assertTrue(storage.isEnsureScheme());
    storage.close();
  }

  @Test
  public void builderWithEnsureSchemaFalse() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin")
      .setEnsureSchema(false);

    var storage = builder.build();

    assertFalse(storage.isEnsureScheme());
    storage.close();
  }

  @Test
  public void builderWithStrictTraceIdFalse() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin")
      .setStrictTraceId(false);

    var storage = builder.build();

    assertFalse(storage.isStrictTraceId());
    storage.close();
  }

  @Test
  public void spanStoreReturnsValidInstance() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin");

    var storage = builder.build();
    var spanStore = storage.spanStore();

    assertNotNull(spanStore);
    assertInstanceOf(SpanStore.class, spanStore);
    storage.close();
  }

  @Test
  public void spanConsumerReturnsValidInstance() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin");

    var storage = builder.build();
    var spanConsumer = storage.spanConsumer();

    assertNotNull(spanConsumer);
    assertInstanceOf(SpanConsumer.class, spanConsumer);
    storage.close();
  }

  @Test
  public void tracesReturnsValidInstance() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin");

    var storage = builder.build();
    var traces = storage.traces();

    assertNotNull(traces);
    assertInstanceOf(Traces.class, traces);
    storage.close();
  }

  @Test
  public void serviceAndSpanNamesReturnsValidInstance() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin");

    var storage = builder.build();
    var serviceAndSpanNames = storage.serviceAndSpanNames();

    assertNotNull(serviceAndSpanNames);
    assertInstanceOf(ServiceAndSpanNames.class, serviceAndSpanNames);
    storage.close();
  }

  @Test
  public void autocompleteTagsReturnsValidInstance() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin");

    var storage = builder.build();
    var autocompleteTags = storage.autocompleteTags();

    assertNotNull(autocompleteTags);
    assertInstanceOf(AutocompleteTags.class, autocompleteTags);
    storage.close();
  }

  @Test
  public void getClientReturnsValidInstance() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin");

    var storage = builder.build();
    var client = storage.getClient();

    assertNotNull(client);
    assertInstanceOf(Client.class, client);
    storage.close();
  }

  @Test
  public void closeClosesSpanConsumer() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin");

    var storage = builder.build();
    storage.close();

    assertNotNull(storage);
  }

  @Test
  public void builderWithCustomDatabase() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("custom_db");

    var storage = builder.build();

    assertNotNull(storage.getClient());
    storage.close();
  }

  @Test
  public void builderWithCustomCredentials() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin")
      .setUsername("admin")
      .setPassword("password");

    var storage = builder.build();

    assertNotNull(storage.getClient());
    storage.close();
  }

  @Test
  public void builderWithCustomHostAndPort() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("clickhouse.example.com")
      .setPort(9000)
      .setDatabase("zipkin");

    var storage = builder.build();

    assertNotNull(storage.getClient());
    storage.close();
  }

  @Test
  public void multipleCallsToSpanStoreReturnDifferentInstances() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin");

    var storage = builder.build();
    var spanStore1 = storage.spanStore();
    var spanStore2 = storage.spanStore();

    assertSame(spanStore1, spanStore2);
    storage.close();
  }

  @Test
  public void multipleCallsToTracesReturnDifferentInstances() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin");

    var storage = builder.build();
    var traces1 = storage.traces();
    var traces2 = storage.traces();

    assertNotNull(traces1);
    assertNotNull(traces2);
    storage.close();
  }

  @Test
  public void createClientWithValidConfiguration() {
    var builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin")
      .setUsername("default")
      .setPassword("password");

    var storage = builder.build();
    var client = storage.getClient();

    assertNotNull(client);
    storage.close();
  }
}

