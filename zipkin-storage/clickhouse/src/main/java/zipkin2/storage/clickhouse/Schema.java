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

  public static void ensure(ClickHouseStorage clickHouseStorage) {
    if (!clickHouseStorage.isEnsureScheme()) return;

    try {
      if (allTablesExist(clickHouseStorage)) {
        return;
      }

      try (InputStream is = Schema.class.getResourceAsStream("/schema/zipkin-schema-1.sql")) {

        if (is == null) {
          throw new IllegalStateException("zipkin-schema-1.sql not found");
        }

        String sqlContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);

        String[] statements = sqlContent.split(";");

        for (String statement : statements) {
          String trimmedStatement = statement.trim();

          if (!trimmedStatement.isEmpty() && !trimmedStatement.startsWith("--")) {
            clickHouseStorage.getClient().execute(trimmedStatement).get();
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize ClickHouse schema", e);
    }
  }

  private static boolean allTablesExist(ClickHouseStorage clickHouseStorage) throws Exception {
    for (String tableName : REQUIRED_TABLES) {
      if (!tableExists(clickHouseStorage, tableName)) {
        return false; // At least one table is missing
      }
    }
    return true; // All tables exist
  }

  private static boolean tableExists(ClickHouseStorage clickHouseStorage, String tableName) throws Exception {
    String query = String.format(
      "SELECT 1 FROM system.tables WHERE database = 'zipkin' AND name = '%s' LIMIT 1",
      tableName
    );

    var response = clickHouseStorage.getClient().query(query).get();
    var client = clickHouseStorage.getClient();

    try (var reader = client.newBinaryFormatReader(response)) {
      return reader.hasNext();
    }
  }
}


