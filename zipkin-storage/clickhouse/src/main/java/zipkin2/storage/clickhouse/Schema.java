package zipkin2.storage.clickhouse;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

class Schema {

  public static void ensure(ClickHouseStorage clickHouseStorage) {
    if (!clickHouseStorage.isEnsureScheme()) return;

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
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize ClickHouse schema", e);
    }
  }
}

