package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.Traces;
import zipkin2.storage.clickhouse.cache.AutocompleteTagsCache;
import zipkin2.storage.clickhouse.dto.DependencyRecord;
import zipkin2.storage.clickhouse.dto.ServiceOperationNameRecord;
import zipkin2.storage.clickhouse.dto.SpanRecord;


public class ClickHouseStorage extends StorageComponent {

  private final ClickHouseSpanStore clickHouseSpanStore;
  private final ClickHouseSpanConsumer spanConsumer;
  private final Client client;
  private final boolean ensureScheme;
  private final String database;
  private final boolean strictTraceId;
  private final Set<String> autocompleteKeys;
  private final int autocompleteTtl;
  private final int autocompleteCardinality;
  private final AutocompleteTagsCache autocompleteTagsCache;
  private final int maxSpansLimitMultiplier;

  ClickHouseStorage(Builder b) {
    this.client = createClient(b);
    this.database = b.database;
    this.strictTraceId = b.strictTraceId;
    this.autocompleteKeys = b.autocompleteKeys;
    this.autocompleteTtl = b.autocompleteTtl;
    this.autocompleteCardinality = b.autocompleteCardinality;
    this.maxSpansLimitMultiplier = b.maxSpansLimitMultiplier;
    this.autocompleteTagsCache = new AutocompleteTagsCache(
      b.autocompleteTtl, b.autocompleteCardinality, b.autocompleteKeys
    );
    this.clickHouseSpanStore = new ClickHouseSpanStore(client, b.database, b.strictTraceId, b.maxSpansLimitMultiplier);
    this.spanConsumer = new ClickHouseSpanConsumer(
      client, b.database, b.strictTraceId, b.autocompleteKeys,
      b.autocompleteTtl, b.autocompleteCardinality, this.autocompleteTagsCache
    );
    this.ensureScheme = b.ensureSchema;
    if (ensureScheme) {
      Schema.ensure(this);
    }
    registerTables();
  }

  @Override
  public SpanStore spanStore() {
    return clickHouseSpanStore;
  }

  @Override
  public SpanConsumer spanConsumer() {
    return spanConsumer;
  }

  @Override
  public Traces traces() {
    return new ClickHouseTraces(client, database);
  }

  @Override
  public ServiceAndSpanNames serviceAndSpanNames() {
    return new ClickHouseServiceAndSpanNames(client, database);
  }

  @Override
  public AutocompleteTags autocompleteTags() {
    return new ClickHouseAutocompleteTags(client, database, autocompleteTagsCache);
  }

  public boolean isEnsureScheme() {
    return ensureScheme;
  }

  public Client getClient() {
    return client;
  }

  public boolean isStrictTraceId() {
    return strictTraceId;
  }


  public Set<String> getAutocompleteKeys() {
    return autocompleteKeys;
  }

  public int getAutocompleteTtl() {
    return autocompleteTtl;
  }

  public int getAutocompleteCardinality() {
    return autocompleteCardinality;
  }

  public AutocompleteTagsCache getAutocompleteTagsCache() {
    return autocompleteTagsCache;
  }

  public void close() {
    if (spanConsumer != null) {
      spanConsumer.close();
    }
  }

  public int getMaxSpansLimitMultiplier() {
    return maxSpansLimitMultiplier;
  }

  public Client createClient(Builder b) {
    return new Client.Builder()
      .addEndpoint("http://" + b.host + ":" + b.port + "/")
      .setUsername(b.username)
      .setPassword(b.password)
      .setDefaultDatabase(b.database)
      .build();
  }

  public void registerTables() {
    client.register(SpanRecord.class, client.getTableSchema("spans"));
    client.register(DependencyRecord.class, client.getTableSchema("dependencies"));
    client.register(ServiceOperationNameRecord.class, client.getTableSchema("service_operation_names"));
  }

  public static class Builder {

    private String host;
    private int port;
    private String database;
    private boolean ensureSchema;
    private String username;
    private String password;
    private boolean strictTraceId = true;
    private Set<String> autocompleteKeys = Set.of();
    private int autocompleteTtl = (int) TimeUnit.HOURS.toMillis(1);
    private int autocompleteCardinality = 5 * 4000;
    private int maxSpansLimitMultiplier = 100;

    public ClickHouseStorage build() {
      return new ClickHouseStorage(this);
    }

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setDatabase(String database) {
      this.database = database;
      return this;
    }

    public Builder setEnsureSchema(boolean ensureSchema) {
      this.ensureSchema = ensureSchema;
      return this;
    }

    public Builder setUsername(String username) {
      this.username = username;
      return this;
    }

    public Builder setPassword(String password) {
      this.password = password;
      return this;
    }

    public Builder setStrictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    public Builder setAutocompleteKeys(List<String> keys) {
      if (keys == null) throw new NullPointerException("keys == null");
      this.autocompleteKeys = Set.copyOf(keys);
      return this;
    }

    public Builder setAutocompleteTtl(int autocompleteTtl) {
      if (autocompleteTtl <= 0) throw new IllegalArgumentException("autocompleteTtl <= 0");
      this.autocompleteTtl = autocompleteTtl;
      return this;
    }

    public Builder setAutocompleteCardinality(int autocompleteCardinality) {
      if (autocompleteCardinality <= 0) {
        throw new IllegalArgumentException("autocompleteCardinality <= 0");
      }
      this.autocompleteCardinality = autocompleteCardinality;
      return this;
    }

    public Builder setMaxSpansLimitMultiplier(int maxSpansLimitMultiplier) {
      if (maxSpansLimitMultiplier <= 0) {
        throw new IllegalArgumentException("maxSpansLimitMultiplier <= 0");
      }
      this.maxSpansLimitMultiplier = maxSpansLimitMultiplier;
      return this;
    }
  }
}
