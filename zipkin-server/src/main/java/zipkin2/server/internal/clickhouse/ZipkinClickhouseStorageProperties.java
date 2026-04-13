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
  private boolean ensureSchema = true;
  private boolean strictTraceId = true;
  private List<String> autocompleteKeys = new ArrayList<>();
  private int autocompleteTtl = (int) TimeUnit.HOURS.toMillis(1);
  private int autocompleteCardinality = 5 * 4000;
  private int maxSpansLimitMultiplier = 100;

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

  public int getAutocompleteCardinality() {
    return autocompleteCardinality;
  }

  public void setAutocompleteCardinality(int autocompleteCardinality) {
    if (autocompleteCardinality <= 0) {
      throw new IllegalArgumentException("autocompleteCardinality <= 0");
    }
    this.autocompleteCardinality = autocompleteCardinality;
  }

  public ClickHouseStorage.Builder toStorageBuilder() {
    return new ClickHouseStorage.Builder()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password)
      .setDatabase(database)
      .setEnsureSchema(ensureSchema)
      .setStrictTraceId(strictTraceId)
      .setAutocompleteKeys(autocompleteKeys)
      .setAutocompleteTtl(autocompleteTtl)
      .setAutocompleteCardinality(autocompleteCardinality)
      .setMaxSpansLimitMultiplier(maxSpansLimitMultiplier);
  }
}
