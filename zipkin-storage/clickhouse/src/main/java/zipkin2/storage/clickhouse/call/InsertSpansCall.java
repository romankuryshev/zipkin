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
  private final boolean strictTraceId;

  public InsertSpansCall(Client client, String database, List<Span> spans, boolean strictTraceId) {
    this.client = client;
    this.database = database;
    this.spans = spans;
    this.strictTraceId = strictTraceId;
  }

  @Override
  public Void execute() {
    try {
      // 1. Insert into spans table
      String spansInsert = buildSpansInsertStatement();
      client.query(spansInsert).get();

      // 2. Insert into service_operation_names table
      String serviceOpsInsert = buildServiceOperationNamesInsertStatement();
      if (!serviceOpsInsert.isEmpty()) {
        client.query(serviceOpsInsert).get();
      }

      // 3. Insert into dependencies table
      String depsInsert = buildDependenciesInsertStatement();
      if (!depsInsert.isEmpty()) {
        client.query(depsInsert).get();
      }
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

  private String buildSpansInsertStatement() {
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ").append(database).append(".spans ")
      .append("(trace_id, trace_id_high, parent_id, span_id, kind, name, timestamp, duration, ")
      .append("local_endpoint_service_name, local_endpoint_ipv4, local_endpoint_ipv6, local_endpoint_port, ")
      .append("remote_endpoint_service_name, remote_endpoint_ipv4, remote_endpoint_ipv6, remote_endpoint_port, ")
      .append("annotations, tags, status_code) ")
      .append("VALUES ");

    for (int i = 0; i < spans.size(); i++) {
      if (i > 0) sb.append(", ");

      Span span = spans.get(i);
      sb.append("(");

      // Handle traceId splitting logic (same as Cassandra)
      boolean traceIdHigh = !strictTraceId && span.traceId().length() == 32;
      String traceIdToUse = traceIdHigh ? span.traceId().substring(16) : span.traceId();
      String traceIdHighStr = traceIdHigh ? span.traceId().substring(0, 16) : null;

      // trace_id (low 64 bits)
      long traceIdLow = parseHexStringToLong(traceIdToUse);
      sb.append(traceIdLow).append(", ");

      // trace_id_high (high 64 bits)
      long traceIdHighVal = traceIdHighStr != null ? parseHexStringToLong(traceIdHighStr) : 0L;
      sb.append(traceIdHighVal).append(", ");

      // parent_id
      String parentId = span.parentId();
      if (parentId != null && !parentId.isEmpty()) {
        sb.append(parseHexStringToLong(parentId));
      } else {
        sb.append("NULL");
      }
      sb.append(", ");

      // span_id
      long spanId = parseHexStringToLong(span.id());
      sb.append(spanId).append(", ");

      // kind
      String spanKind = span.kind() != null ? span.kind().toString() : "";
      sb.append(quoteString(spanKind)).append(", ");

      // name
      sb.append(quoteString(span.name() != null ? span.name() : "")).append(", ");

      // timestamp
      long timestamp = span.timestampAsLong() > 0 ? span.timestampAsLong() : 0;
      sb.append(timestamp).append(", ");

      // duration
      long duration = span.durationAsLong() > 0 ? span.durationAsLong() : 0;
      sb.append(duration).append(", ");

      // local_endpoint fields
      if (span.localEndpoint() != null) {
        String serviceName = span.localEndpoint().serviceName();
        sb.append(quoteString(serviceName != null ? serviceName : "")).append(", ");

        String ipv4 = span.localEndpoint().ipv4();
        sb.append(quoteNullableString(ipv4)).append(", ");

        String ipv6 = span.localEndpoint().ipv6();
        sb.append(quoteNullableString(ipv6)).append(", ");

        Integer port = span.localEndpoint().port();
        if (port != null && port > 0) {
          sb.append(port);
        } else {
          sb.append("NULL");
        }
      } else {
        sb.append("'', NULL, NULL, NULL");
      }
      sb.append(", ");

      // remote_endpoint fields
      if (span.remoteEndpoint() != null) {
        String serviceName = span.remoteEndpoint().serviceName();
        sb.append(quoteString(serviceName != null ? serviceName : "")).append(", ");

        String ipv4 = span.remoteEndpoint().ipv4();
        sb.append(quoteNullableString(ipv4)).append(", ");

        String ipv6 = span.remoteEndpoint().ipv6();
        sb.append(quoteNullableString(ipv6)).append(", ");

        Integer port = span.remoteEndpoint().port();
        if (port != null && port > 0) {
          sb.append(port);
        } else {
          sb.append("NULL");
        }
      } else {
        sb.append("'', NULL, NULL, NULL");
      }
      sb.append(", ");

      // annotations (Array)
      sb.append(buildAnnotationsArray(span)).append(", ");

      // tags (Map)
      sb.append(buildTagsMap(span.tags())).append(", ");

      // status_code
      String statusCode = span.tags().get("status.code");
      sb.append(quoteNullableString(statusCode));

      sb.append(")");
    }

    return sb.toString();
  }

  private String buildServiceOperationNamesInsertStatement() {
    java.util.Set<String> uniquePairs = new java.util.HashSet<>();

    for (Span span : spans) {
      String serviceName = getServiceName(span);
      String operationName = span.name();

      if (serviceName != null && !serviceName.isEmpty() &&
          operationName != null && !operationName.isEmpty()) {
        uniquePairs.add(serviceName + "|" + operationName);
      }
    }

    if (uniquePairs.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ").append(database).append(".service_operation_names ")
      .append("(service_name, operation_name) ")
      .append("VALUES ");

    boolean first = true;
    for (String pair : uniquePairs) {
      if (!first) sb.append(", ");
      String[] parts = pair.split("\\|", 2);
      sb.append("(").append(quoteString(parts[0])).append(", ")
        .append(quoteString(parts[1])).append(")");
      first = false;
    }

    return sb.toString();
  }

  private String buildDependenciesInsertStatement() {
    java.util.Set<String> uniqueDeps = new java.util.HashSet<>();

    for (Span span : spans) {
      String localService = getServiceName(span);
      String remoteService = null;

      if (span.remoteEndpoint() != null && span.remoteEndpoint().serviceName() != null) {
        remoteService = span.remoteEndpoint().serviceName();
      }

      if (localService != null && !localService.isEmpty() &&
          remoteService != null && !remoteService.isEmpty()) {
        uniqueDeps.add(localService + "|" + remoteService);
      }
    }

    if (uniqueDeps.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ").append(database).append(".dependencies ")
      .append("(local_service_name, remote_service_name) ")
      .append("VALUES ");

    boolean first = true;
    for (String dep : uniqueDeps) {
      if (!first) sb.append(", ");
      String[] parts = dep.split("\\|", 2);
      sb.append("(").append(quoteString(parts[0])).append(", ")
        .append(quoteString(parts[1])).append(")");
      first = false;
    }

    return sb.toString();
  }

  private long parseHexStringToLong(String hexStr) {
    if (hexStr == null || hexStr.isEmpty()) return 0L;
    try {
      return Long.parseUnsignedLong(hexStr, 16);
    } catch (Exception e) {
      return 0L;
    }
  }

  private String buildAnnotationsArray(Span span) {
    if (span.annotations() == null || span.annotations().isEmpty()) {
      return "[]";
    }

    StringBuilder sb = new StringBuilder("[");
    boolean first = true;

    for (zipkin2.Annotation annotation : span.annotations()) {
      if (!first) sb.append(", ");
      sb.append("(").append(annotation.timestamp()).append(", ")
        .append(quoteString(annotation.value())).append(")");
      first = false;
    }

    sb.append("]");
    return sb.toString();
  }


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
    return "'" + str.replace("'", "''") + "'";
  }

  private String quoteNullableString(String str) {
    if (str == null || str.isEmpty()) return "NULL";
    return quoteString(str);
  }
}
