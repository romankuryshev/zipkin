package zipkin2.storage.clickhouse;

import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.testcontainers.utility.DockerImageName.parse;

class ClickHouseContainer extends GenericContainer<ClickHouseContainer> {
  static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseContainer.class);
  static final int PORT = 8123;

  ClickHouseContainer() {
    super(parse("clickhouse/clickhouse-server:24.3-alpine"));
    addExposedPort(PORT);
    withEnv("CLICKHOUSE_USER", "default");
    withEnv("CLICKHOUSE_PASSWORD", "");
    withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1");
    waitStrategy = Wait.forHttp("/ping").forPort(PORT).withStartupTimeout(Duration.ofMinutes(2));
    withLogConsumer(new Slf4jLogConsumer(LOGGER));
  }

  @Override public void start() {
    super.start();
    initSchema();
    LOGGER.info("ClickHouse ready at {}:{}", getHost(), getMappedPort(PORT));
  }

  ClickHouseStorage.Builder newStorageBuilder() {
    return new ClickHouseStorage.Builder()
      .setHost(getHost())
      .setPort(getMappedPort(PORT))
      .setDatabase("zipkin")
      .setUsername("default")
      .setPassword("")
      .setEnsureSchema(false)
      .setIncludeSpanStatistics(false);
  }

  void clear() throws Exception {
    for (String table : List.of("spans", "spans_aggregate_stats", "service_operation_names", "dependencies")) {
      execInContainer("clickhouse-client", "--database", "zipkin",
        "--query", "TRUNCATE TABLE IF EXISTS " + table);
    }
  }

  private void initSchema() {
    try {
      execInContainer("clickhouse-client",
        "--query", "CREATE DATABASE IF NOT EXISTS zipkin");

      copyFileToContainer(
        org.testcontainers.utility.MountableFile.forClasspathResource("/schema/zipkin-schema-1.sql"),
        "/tmp/schema.sql"
      );
      execInContainer("sh", "-c",
        "clickhouse-client --database zipkin --multiquery < /tmp/schema.sql");
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize ClickHouse schema", e);
    }
  }
}
