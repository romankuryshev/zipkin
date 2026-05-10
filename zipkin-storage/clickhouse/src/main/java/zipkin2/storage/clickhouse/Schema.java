package zipkin2.storage.clickhouse;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

class Schema {

  private static final List<String> REQUIRED_TABLES = Arrays.asList(
    "service_operation_names",
    "dependencies",
    "spans"
  );

  public static void ensure(ClickHouseStorage storage) {
    if (!storage.isEnsureScheme()) return;

    try {
      if (allTablesExist(storage)) {
        return;
      }

      boolean isCluster = storage.getClusterName() != null;
      String schemaResource = isCluster
        ? "/schema/zipkin-schema-cluster.sql"
        : "/schema/zipkin-schema-1.sql";

      try (InputStream is = Schema.class.getResourceAsStream(schemaResource)) {
        if (is == null) {
          throw new IllegalStateException(schemaResource + " not found");
        }

        String sqlContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);

        if (isCluster) {
          sqlContent = sqlContent.replace("{cluster}", storage.getClusterName());
        }

        String[] statements = sqlContent.split(";");

        for (String statement : statements) {
          String trimmedStatement = statement.trim();
          if (isOnlyComments(trimmedStatement)) continue;
          storage.getClient().execute(trimmedStatement).get();
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize ClickHouse schema", e);
    }
  }

  private static boolean isOnlyComments(String statement) {
    return Arrays.stream(statement.split("\n"))
      .map(String::trim)
      .filter(line -> !line.isEmpty())
      .allMatch(line -> line.startsWith("--"));
  }

  private static boolean allTablesExist(ClickHouseStorage storage) throws Exception {
    for (String tableName : REQUIRED_TABLES) {
      if (!tableExists(storage, tableName)) {
        return false;
      }
    }
    return true;
  }

  private static boolean tableExists(ClickHouseStorage storage, String tableName) throws Exception {
    String query = String.format(
      "SELECT 1 FROM system.tables WHERE database = '%s' AND name = '%s' LIMIT 1",
      storage.getDatabase(), tableName
    );

    var response = storage.getClient().query(query).get();
    var client = storage.getClient();

    try (var reader = client.newBinaryFormatReader(response)) {
      return reader.hasNext();
    }
  }
}
