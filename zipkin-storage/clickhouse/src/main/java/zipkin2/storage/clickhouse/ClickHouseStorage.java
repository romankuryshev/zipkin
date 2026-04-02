package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.Traces;
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

  ClickHouseStorage(Builder b) {
    this.client = createClient(b);
    this.clickHouseSpanStore = new ClickHouseSpanStore(client, b.database, b.strictTraceId);
    this.spanConsumer = new ClickHouseSpanConsumer(client, b.database, b.strictTraceId);
    this.database = b.database;
    this.strictTraceId = b.strictTraceId;
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
    return new ClickHouseAutocompleteTags(client, database);
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

  public void close() {
    if (spanConsumer != null) {
      spanConsumer.close();
    }
  }

  public Client createClient(Builder b) {
    Client c = new Client.Builder()
      .addEndpoint("http://" + b.host + ":" + b.port + "/")
      .setUsername(b.username)
      .setPassword(b.password)
      .setDefaultDatabase(b.database)
      .build();
    return c;
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
  }
}
