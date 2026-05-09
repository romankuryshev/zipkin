package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.Traces;
import java.io.IOException;
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
  private final boolean includeSpanStatistics;

  ClickHouseStorage(Builder b) {
    this.client = createClient(b);
    this.database = b.database;
    this.strictTraceId = b.strictTraceId;
    this.autocompleteKeys = b.autocompleteKeys;
    this.autocompleteTtl = b.autocompleteTtl;
    this.autocompleteCardinality = b.autocompleteCardinality;
    this.maxSpansLimitMultiplier = b.maxSpansLimitMultiplier;
    this.includeSpanStatistics = b.includeSpanStatistics;
    this.autocompleteTagsCache = new AutocompleteTagsCache(
      b.autocompleteTtl, b.autocompleteCardinality, b.autocompleteKeys
    );
    this.clickHouseSpanStore = new ClickHouseSpanStore(client, b.database, b.strictTraceId, b.maxSpansLimitMultiplier, b.includeSpanStatistics);
    this.spanConsumer = new ClickHouseSpanConsumer(
      client, b.strictTraceId, b.autocompleteKeys, this.autocompleteTagsCache
    );
    this.ensureScheme = b.ensureSchema;
    if (ensureScheme) {
      Schema.ensure(this);
    }
    registerTables();
  }

  // Package-private constructor for testing with a pre-built mock client.
  // Skips registerTables() and ensureSchema.
  ClickHouseStorage(Builder b, Client client) {
    this.client = client;
    this.database = b.database;
    this.strictTraceId = b.strictTraceId;
    this.autocompleteKeys = b.autocompleteKeys;
    this.autocompleteTtl = b.autocompleteTtl;
    this.autocompleteCardinality = b.autocompleteCardinality;
    this.maxSpansLimitMultiplier = b.maxSpansLimitMultiplier;
    this.includeSpanStatistics = b.includeSpanStatistics;
    this.autocompleteTagsCache = new AutocompleteTagsCache(
      b.autocompleteTtl, b.autocompleteCardinality, b.autocompleteKeys
    );
    this.clickHouseSpanStore = new ClickHouseSpanStore(client, b.database, b.strictTraceId, b.maxSpansLimitMultiplier, b.includeSpanStatistics);
    this.spanConsumer = new ClickHouseSpanConsumer(
      client, b.strictTraceId, b.autocompleteKeys, this.autocompleteTagsCache
    );
    this.ensureScheme = false;
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
    return new ClickHouseTraces(client, database, includeSpanStatistics);
  }

  @Override
  public ServiceAndSpanNames serviceAndSpanNames() {
    return new ClickHouseServiceAndSpanNames(client, database);
  }

  @Override
  public AutocompleteTags autocompleteTags() {
    return new ClickHouseAutocompleteTags(client, database, autocompleteKeys, autocompleteTagsCache);
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

  // Package-private: called from integration tests via blockWhileInFlight() to ensure
  // buffered spans are flushed to ClickHouse before read assertions.
  void forceFlush() throws IOException {
    spanConsumer.forceFlush();
  }

  public void close() {
    if (spanConsumer != null) {
      spanConsumer.close();
    }
  }

  public int getMaxSpansLimitMultiplier() {
    return maxSpansLimitMultiplier;
  }

  public boolean isIncludeSpanStatistics() {
    return includeSpanStatistics;
  }

  public Client createClient(Builder b) {
    Client.Builder clientBuilder = new Client.Builder()
      .setUsername(b.username)
      .setPassword(b.password)
      .setDefaultDatabase(b.database);

    if (!b.clusterNodes.isEmpty()) {
      for (String endpoint : b.clusterNodes) {
        clientBuilder.addEndpoint(endpoint);
      }
    } else {
      clientBuilder.addEndpoint("http://" + b.host + ":" + b.port + "/");
    }

    return clientBuilder.build();
  }

  public void registerTables() {
    client.register(SpanRecord.class, client.getTableSchema("spans"));
    client.register(DependencyRecord.class, client.getTableSchema("dependencies"));
    client.register(ServiceOperationNameRecord.class, client.getTableSchema("service_operation_names"));
  }

  public static class Builder extends StorageComponent.Builder {

    private String host;
    private int port;
    private List<String> clusterNodes = new ArrayList<>();
    private String database;
    private boolean ensureSchema;
    private String username;
    private String password;
    private boolean strictTraceId = true;
    private Set<String> autocompleteKeys = Set.of();
    private int autocompleteTtl = (int) TimeUnit.HOURS.toMillis(1);
    private int autocompleteCardinality = 5 * 4000;
    private int maxSpansLimitMultiplier = 100;
    private boolean includeSpanStatistics = true;

    @Override public ClickHouseStorage build() {
      return new ClickHouseStorage(this);
    }

    // --- StorageComponent.Builder contract ---

    @Override public Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    @Override public Builder searchEnabled(boolean searchEnabled) {
      // ClickHouse storage does not distinguish search-enabled vs disabled yet.
      return this;
    }

    @Override public Builder autocompleteKeys(List<String> keys) {
      return setAutocompleteKeys(keys);
    }

    @Override public Builder autocompleteTtl(int autocompleteTtl) {
      return setAutocompleteTtl(autocompleteTtl);
    }

    @Override public Builder autocompleteCardinality(int autocompleteCardinality) {
      return setAutocompleteCardinality(autocompleteCardinality);
    }

    // --- ClickHouse-specific builder methods ---

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
      return strictTraceId(strictTraceId);
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

    public Builder setIncludeSpanStatistics(boolean includeSpanStatistics) {
      this.includeSpanStatistics = includeSpanStatistics;
      return this;
    }

    public Builder addClusterNode(String host, int port) {
      if (host == null) throw new NullPointerException("host == null");
      if (port <= 0) throw new IllegalArgumentException("port <= 0");
      this.clusterNodes.add("http://" + host + ":" + port + "/");
      return this;
    }

    public Builder setClusterNodes(List<String> nodes) {
      if (nodes == null) throw new NullPointerException("nodes == null");
      if (nodes.isEmpty()) throw new IllegalArgumentException("nodes is empty");
      this.clusterNodes.clear();
      this.clusterNodes.addAll(nodes);
      return this;
    }
  }
}
