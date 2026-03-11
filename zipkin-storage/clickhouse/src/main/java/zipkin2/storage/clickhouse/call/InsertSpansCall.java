package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.Span;

import java.util.List;

/**
 * Async call implementation for inserting spans into ClickHouse.
 * Uses ClickHouse Client v2 API: client.query(sql).execute().get()
 */
public final class InsertSpansCall extends Call<Void> {
  private final Client client;
  private final String database;
  private final List<Span> spans;

  public InsertSpansCall(Client client, String database, List<Span> spans) {
    this.client = client;
    this.database = database;
    this.spans = spans;
  }

  @Override
  public Void execute() {
    String sql = buildInsertStatement();

    // ClickHouse Client v2 API: execute().get() - returns CompletableFuture
    // The .get() blocks until the query is complete
    try {
      client.query(sql).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return null;
  }

  @Override
  public void enqueue(zipkin2.Callback<Void> callback) {
    try {
      execute();
      callback.onSuccess(null);
    } catch (Throwable e) {
      callback.onError(e);
    }
  }

  @Override
  public void cancel() {
    // ClickHouse client v2 doesn't support cancellation
  }

  @Override
  public boolean isCanceled() {
    return false;
  }

  @Override
  public Call<Void> clone() {
    return this;
  }

  @Override
  public String toString() {
    return "InsertSpans{count=" + spans.size() + "}";
  }

  /**
   * Builds the INSERT INTO statement with all span records.
   * Uses ClickHouse native SQL syntax.
   */
  private String buildInsertStatement() {
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ").append(database).append(".spans ")
      .append("(trace_id, span_id, parent_id, service_name, operation_name, ")
      .append("remote_service_name, span_kind, start_time, duration_us, tags, status_code) ")
      .append("VALUES ");

    for (int i = 0; i < spans.size(); i++) {
      if (i > 0) sb.append(", ");

      Span span = spans.get(i);
      sb.append("(");

      // trace_id
      sb.append(convertHexStringToDecimal(span.traceId())).append(", ");

      // span_id
      sb.append(convertHexStringToDecimal(span.id())).append(", ");

      // parent_id
      sb.append(convertHexStringToDecimal(span.parentId())).append(", ");

      // service_name
      String serviceName = getServiceName(span);
      sb.append(quoteString(serviceName != null ? serviceName : "")).append(", ");

      // operation_name
      sb.append(quoteString(span.name() != null ? span.name() : "")).append(", ");

      // remote_service_name
      sb.append(quoteNullableString(span.remoteServiceName())).append(", ");

      // span_kind
      String spanKind = span.kind() != null ? span.kind().toString() : "";
      sb.append(quoteString(spanKind)).append(", ");

      // start_time (DateTime64(6) - microseconds, ClickHouse stores as UInt64)
      long timestamp = span.timestampAsLong() > 0 ? span.timestampAsLong() : 0;
      sb.append(timestamp).append(", ");

      // duration_us
      long duration = span.durationAsLong() > 0 ? span.durationAsLong() : 0;
      sb.append(duration).append(", ");

      // tags (Map(String, String))
      sb.append(buildTagsMap(span.tags())).append(", ");

      // status_code
      String statusCode = span.tags().get("status.code");
      sb.append(quoteNullableString(statusCode));

      sb.append(")");
    }

    return sb.toString();
  }

  private String convertHexStringToDecimal(String hexStr) {
    if (hexStr == null || hexStr.isEmpty()) return "NULL";
    try {
      long decimal = Long.parseUnsignedLong(hexStr, 16);
      return String.valueOf(decimal);
    } catch (Exception e) {
      return "NULL";
    }
  }

  /**
   * Builds ClickHouse Map representation from Java Map.
   * Format: {'key1': 'value1', 'key2': 'value2'}
   */
  private String buildTagsMap(java.util.Map<String, String> tags) {
    if (tags.isEmpty()) return "{}";

    StringBuilder sb = new StringBuilder("{");
    boolean first = true;

    for (java.util.Map.Entry<String, String> entry : tags.entrySet()) {
      if (!first) sb.append(", ");
      sb.append(quoteString(entry.getKey())).append(": ")
        .append(quoteString(entry.getValue()));
      first = false;
    }

    sb.append("}");
    return sb.toString();
  }

  private String getServiceName(Span span) {
    if (span.localEndpoint() != null && span.localEndpoint().serviceName() != null) {
      return span.localEndpoint().serviceName();
    }
    return span.tags().get("service");
  }

  private String quoteString(String str) {
    if (str == null) return "''";
    // Escape single quotes by doubling them (ClickHouse standard)
    return "'" + str.replace("'", "''") + "'";
  }

  private String quoteNullableString(String str) {
    if (str == null || str.isEmpty()) return "NULL";
    return quoteString(str);
  }
}
