package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Маппер для преобразования результатов ClickHouse в объекты Zipkin
 */
public final class ClickHouseResultMapper {

  private ClickHouseResultMapper() {}

  /**
   * Преобразует одну строку результата в объект Span
   */
  static Span toSpan(Map<String, Object> record) {
    Span.Builder builder = Span.newBuilder();

    builder.traceId(fromBigInteger((BigInteger) record.get("trace_id")));
    builder.id(fromBigInteger((BigInteger) record.get("span_id")));

    BigInteger parentId = (BigInteger) record.get("parent_id");
    if (parentId != null) {
      builder.parentId(fromBigInteger(parentId));
    }

    builder.name((String) record.get("operation_name"));

    String serviceName = (String) record.get("service_name");
    if (serviceName != null && !serviceName.isEmpty()) {
      builder.localEndpoint(Endpoint.newBuilder().serviceName(serviceName).build());
    }

    String remoteService = (String) record.get("remote_service_name");
    if (remoteService != null && !remoteService.isEmpty()) {
      builder.remoteEndpoint(Endpoint.newBuilder().serviceName(remoteService).build());
    }

    String spanKind = (String) record.get("span_kind");
    if (spanKind != null && !spanKind.isEmpty()) {
      try {
        builder.kind(Span.Kind.valueOf(spanKind));
      } catch (IllegalArgumentException ignored) {
      }
    }

    Long timestamp = getLong(record.get("start_time"));
    if (timestamp != null && timestamp > 0) {
      builder.timestamp(timestamp);
    }

    Long duration = getLong(record.get("duration_us"));
    if (duration != null && duration > 0) {
      builder.duration(duration);
    }

    String statusCode = (String) record.get("status_code");
    if (statusCode != null && !statusCode.isEmpty()) {
      builder.putTag("status.code", statusCode);
    }

    @SuppressWarnings("unchecked")
    Map<String, String> tags = (Map<String, String>) record.get("tags");
    if (tags != null) {
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        builder.putTag(tag.getKey(), tag.getValue());
      }
    }

    return builder.build();
  }

  /**
   * Преобразует результат запроса в список Span объектов
   */
  static List<Span> toSpans(QueryResponse response, Client client) {
    List<Span> spans = new ArrayList<>();

    try (ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
      while (reader.hasNext()) {
        Map<String, Object> record = reader.next();;
        spans.add(toSpan(record));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read spans from ClickHouse", e);
    }

    return spans;
  }

  /**
   * Преобразует результат запроса в список строк
   */
  static List<String> toStringList(QueryResponse response, Client client, String columnName) {
    List<String> result = new ArrayList<>();

    try (ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
      while (reader.hasNext()) {
        Map<String, Object> record = reader.next();
        String value = (String) record.get(columnName);
        if (value != null && !value.isEmpty()) {
          result.add(value);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read strings from ClickHouse", e);
    }
    return result;
  }

  static String fromBigInteger(BigInteger value) {
    if (value == null) return null;
    return value.toString();
  }

  /**
   * Группирует spans по trace_id
   */
  static List<List<Span>> groupSpansByTraceId(List<Span> spans) {
    Map<String, List<Span>> grouped = spans.stream()
      .collect(Collectors.groupingBy(Span::traceId));
    return new ArrayList<>(grouped.values());
  }

  /**
   * Извлекает зависимости между сервисами из spans
   */
  static List<DependencyLink> extractDependencies(List<Span> spans) {
    if (spans.isEmpty()) return Collections.emptyList();
    DependencyLinker linker = new DependencyLinker();
    linker.putTrace(spans);
    return linker.link();
  }

  private static Long getLong(Object value) {
    if (value == null) return null;
    if (value instanceof Long) return (Long) value;
    if (value instanceof Integer) return ((Integer) value).longValue();
    if (value instanceof BigInteger) return ((BigInteger) value).longValue();
    if (value instanceof String) {
      try {
        return Long.parseLong((String) value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
