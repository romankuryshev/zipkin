package zipkin2.storage.clickhouse.call;

import zipkin2.Endpoint;
import zipkin2.Span;

import java.util.Map;

/**
 * Converts ClickHouse database rows to Span objects
 */
final class SpanRowMapper {

  private SpanRowMapper() {}

  static Span toSpan(var record) {
    Span.Builder builder = Span.newBuilder();

    builder.traceId(record.get("trace_id", String.class));
    builder.id(record.get("span_id", String.class));

    String parentId = record.get("parent_id", String.class);
    if (parentId != null && !parentId.isEmpty()) {
      builder.parentId(parentId);
    }

    builder.name(record.get("operation_name", String.class));

    String serviceName = record.get("service_name", String.class);
    if (serviceName != null && !serviceName.isEmpty()) {
      builder.localEndpoint(Endpoint.newBuilder().serviceName(serviceName).build());
    }

    String remoteService = record.get("remote_service_name", String.class);
    if (remoteService != null && !remoteService.isEmpty()) {
      builder.remoteEndpoint(Endpoint.newBuilder().serviceName(remoteService).build());
    }

    String spanKind = record.get("span_kind", String.class);
    if (spanKind != null && !spanKind.isEmpty()) {
      try {
        builder.kind(Span.Kind.valueOf(spanKind));
      } catch (IllegalArgumentException ignored) {}
    }

    Long timestamp = record.get("start_time", Long.class);
    if (timestamp != null && timestamp > 0) {
      builder.timestamp(timestamp);
    }

    Long duration = record.get("duration_us", Long.class);
    if (duration != null && duration > 0) {
      builder.duration(duration);
    }

    String statusCode = record.get("status_code", String.class);
    if (statusCode != null && !statusCode.isEmpty()) {
      builder.putTag("status.code", statusCode);
    }

    @SuppressWarnings("unchecked")
    Map<String, String> tags = record.get("tags", Map.class);
    if (tags != null) {
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        builder.putTag(tag.getKey(), tag.getValue());
      }
    }

    return builder.build();
  }
}
