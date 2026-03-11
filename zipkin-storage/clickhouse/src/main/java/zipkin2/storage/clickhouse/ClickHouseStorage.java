package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;


public class ClickHouseStorage extends StorageComponent {

  private final ClickHouseSpanStore clickHouseSpanStore;
  private final Client client;
  private final boolean ensureScheme;
  private final String database;

  ClickHouseStorage(Builder b) {
    this.clickHouseSpanStore = new ClickHouseSpanStore();
    this.client = new Client.Builder()
      .addEndpoint("http://" + b.host + ":" + b.port + "/")
      .setUsername(b.username)
      .setPassword(b.password)
      .setDefaultDatabase(b.database)
      .build();
    this.database = b.database;
    this.ensureScheme = b.ensureSchema;
    if (ensureScheme) {
      Schema.ensure(this, client);
    }
  }

  @Override
  public SpanStore spanStore() {
    return clickHouseSpanStore;
  }

  @Override
  public SpanConsumer spanConsumer() {
    return new ClickHouseSpanConsumer(client, database);
  }

  public boolean isEnsureScheme() {
    return ensureScheme;
  }

  public Client getClient() {
    return client;
  }

  public static class Builder {

    private String host;
    private int port;
    private String database;
    private boolean ensureSchema;
    private String username;
    private String password;

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
  }
}
