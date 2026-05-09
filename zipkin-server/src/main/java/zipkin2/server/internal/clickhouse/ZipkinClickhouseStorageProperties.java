package zipkin2.server.internal.clickhouse;

import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin2.storage.clickhouse.ClickHouseStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@ConfigurationProperties("zipkin.storage.clickhouse")
class ZipkinClickhouseStorageProperties {

  private String database = "zipkin";
  private String host = "localhost";
  private int port = 8123;
  private String username = "zipkin";
  private String password = "zipkin";
  private List<String> clusterNodes = new ArrayList<>();
  private boolean ensureSchema = true;
  private boolean strictTraceId = true;
  private List<String> autocompleteKeys = new ArrayList<>();
  private int autocompleteTtl = (int) TimeUnit.HOURS.toMillis(1);
  private boolean includeSpanStatistics = true;
  private int autocompleteCardinality = 5 * 4000;
  private int maxSpansLimitMultiplier = 100;
  private int batchSize = 10000;
  private int autoFlushIntervalMs = 5000;

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public List<String> getClusterNodes() {
    return clusterNodes;
  }

  public void setClusterNodes(List<String> clusterNodes) {
    this.clusterNodes = clusterNodes;
  }

  public boolean isEnsureSchema() {
    return ensureSchema;
  }

  public void setEnsureSchema(boolean ensureSchema) {
    this.ensureSchema = ensureSchema;
  }

  public boolean isStrictTraceId() {
    return strictTraceId;
  }

  public void setStrictTraceId(boolean strictTraceId) {
    this.strictTraceId = strictTraceId;
  }


  public List<String> getAutocompleteKeys() {
    return autocompleteKeys;
  }

  public void setAutocompleteKeys(List<String> autocompleteKeys) {
    this.autocompleteKeys = autocompleteKeys;
  }

  public int getAutocompleteTtl() {
    return autocompleteTtl;
  }

  public void setAutocompleteTtl(int autocompleteTtl) {
    if (autocompleteTtl <= 0) throw new IllegalArgumentException("autocompleteTtl <= 0");
    this.autocompleteTtl = autocompleteTtl;
  }

  public boolean isIncludeSpanStatistics() {
    return includeSpanStatistics;
  }

  public void setIncludeSpanStatistics(boolean includeSpanStatistics) {
    this.includeSpanStatistics = includeSpanStatistics;
  }

  public int getAutocompleteCardinality() {
    return autocompleteCardinality;
  }

  public void setAutocompleteCardinality(int autocompleteCardinality) {
    if (autocompleteCardinality <= 0) {
      throw new IllegalArgumentException("autocompleteCardinality <= 0");
    }
    this.autocompleteCardinality = autocompleteCardinality;
  }

  public int getMaxSpansLimitMultiplier() {
    return maxSpansLimitMultiplier;
  }

  public void setMaxSpansLimitMultiplier(int maxSpansLimitMultiplier) {
    if (maxSpansLimitMultiplier <= 0) {
      throw new IllegalArgumentException("maxSpansLimitMultiplier <= 0");
    }
    this.maxSpansLimitMultiplier = maxSpansLimitMultiplier;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    if (batchSize <= 0) throw new IllegalArgumentException("batchSize <= 0");
    this.batchSize = batchSize;
  }

  public int getAutoFlushIntervalMs() {
    return autoFlushIntervalMs;
  }

  public void setAutoFlushIntervalMs(int autoFlushIntervalMs) {
    if (autoFlushIntervalMs <= 0) throw new IllegalArgumentException("autoFlushIntervalMs <= 0");
    this.autoFlushIntervalMs = autoFlushIntervalMs;
  }

  public ClickHouseStorage.Builder toStorageBuilder() {
    return new ClickHouseStorage.Builder()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password)
      .setDatabase(database)
      .setClusterNodes(clusterNodes)
      .setEnsureSchema(ensureSchema)
      .setStrictTraceId(strictTraceId)
      .setAutocompleteKeys(autocompleteKeys)
      .setAutocompleteTtl(autocompleteTtl)
      .setIncludeSpanStatistics(includeSpanStatistics)
      .setAutocompleteCardinality(autocompleteCardinality)
      .setMaxSpansLimitMultiplier(maxSpansLimitMultiplier)
      .setBatchSize(batchSize)
      .setAutoFlushIntervalMs(autoFlushIntervalMs);
  }
}
