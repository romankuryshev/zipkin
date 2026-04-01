package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.Instant;
import java.time.ZonedDateTime;
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

    // trace_id: combine trace_id and trace_id_high
    long traceIdLow = getLong(record.get("trace_id"));
    long traceIdHigh = getLong(record.get("trace_id_high"));
    String traceId = combineTraceId(traceIdLow, traceIdHigh);
    builder.traceId(traceId);

    // span_id
    long spanIdLong = getLong(record.get("span_id"));
    builder.id(Long.toHexString(spanIdLong));

    // parent_id
    Long parentIdLong = getLong(record.get("parent_id"));
    if (parentIdLong != null && parentIdLong > 0) {
      builder.parentId(Long.toHexString(parentIdLong));
    }

    // name
    builder.name((String) record.get("name"));

    // local_endpoint (flat schema)
    String localServiceName = (String) record.get("local_endpoint_service_name");
    Inet4Address localIpv4 = (Inet4Address) record.get("local_endpoint_ipv4");
    Inet6Address localIpv6 = (Inet6Address) record.get("local_endpoint_ipv6");
    Integer localPort = getInteger(record.get("local_endpoint_port"));

    if (localServiceName != null || localIpv4 != null || localIpv6 != null || localPort != null) {
      Endpoint.Builder localEndpointBuilder = Endpoint.newBuilder();
      if (localServiceName != null && !localServiceName.isEmpty()) {
        localEndpointBuilder.serviceName(localServiceName);
      }
      if (localIpv4 != null) {
        localEndpointBuilder.parseIp(localIpv4);
      }
      if (localIpv6 != null) {
        localEndpointBuilder.parseIp(localIpv6);
      }
      if (localPort != null) {
        localEndpointBuilder.port(localPort);
      }
      builder.localEndpoint(localEndpointBuilder.build());
    }

    // remote_endpoint (flat schema)
    String remoteServiceName = (String) record.get("remote_endpoint_service_name");
    Inet4Address remoteIpv4 = (Inet4Address) record.get("remote_endpoint_ipv4");
    Inet6Address remoteIpv6 = (Inet6Address) record.get("remote_endpoint_ipv6");
    Integer remotePort = getInteger(record.get("remote_endpoint_port"));

    if (remoteServiceName != null || remoteIpv4 != null || remoteIpv6 != null || remotePort != null) {
      Endpoint.Builder remoteEndpointBuilder = Endpoint.newBuilder();
      if (remoteServiceName != null && !remoteServiceName.isEmpty()) {
        remoteEndpointBuilder.serviceName(remoteServiceName);
      }
      if (remoteIpv4 != null) {
        remoteEndpointBuilder.parseIp(remoteIpv4);
      }
      if (remoteIpv6 != null) {
        remoteEndpointBuilder.parseIp(remoteIpv6);
      }
      if (remotePort != null) {
        remoteEndpointBuilder.port(remotePort);
      }
      builder.remoteEndpoint(remoteEndpointBuilder.build());
    }

    String spanKind = (String) record.get("kind");
    if (spanKind != null && !spanKind.isEmpty()) {
      try {
        builder.kind(Span.Kind.valueOf(spanKind));
      } catch (IllegalArgumentException ignored) {
      }
    }

    Long timestamp = getLong(record.get("timestamp"));
    if (timestamp != null && timestamp > 0) {
      builder.timestamp(timestamp);
    }

    Long duration = getLong(record.get("duration"));
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

    List<Object> annotationsList = ((BinaryStreamReader.ArrayValue) record.get("annotations")).asList();
    if (annotationsList != null && !annotationsList.isEmpty()) {
      for (Object annObj : annotationsList) {
        // Annotations are stored as tuples (timestamp, value)
        if (annObj instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, Object> annMap = (Map<String, Object>) annObj;
          long annTimestamp = getLong(annMap.get("timestamp"));
          String annValue = (String) annMap.get("value");
          builder.addAnnotation(annTimestamp, annValue);
        }
      }
    }

    return builder.build();
  }

  /**
   * Combines 64-bit trace_id parts into 128-bit hex string
   */
  private static String combineTraceId(long traceIdLow, long traceIdHigh) {
    if (traceIdHigh == 0L) {
      return Long.toHexString(traceIdLow);
    } else {
      return Long.toHexString(traceIdHigh) + String.format("%016x", traceIdLow);
    }
  }

  /**
   * Преобразует результат запроса в список Span объектов
   */
  static List<Span> toSpans(QueryResponse response, Client client) {
    List<Span> spans = new ArrayList<>();

    try (ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
      while (reader.hasNext()) {
        Map<String, Object> record = reader.next();
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

  /**
   * Группирует spans по trace_id
   */
  static List<List<Span>> groupSpansByTraceId(List<Span> spans) {
    Map<String, List<Span>> grouped = spans.stream()
      .collect(Collectors.groupingBy(Span::traceId));
    return new ArrayList<>(grouped.values());
  }

  static List<DependencyLink> toDependencyLinks(QueryResponse response, Client client) {
    List<DependencyLink> links = new ArrayList<>();

    try (ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
      while (reader.hasNext()) {
        Map<String, Object> record = reader.next();
        String parent = (String) record.get("local_service_name");
        String child = (String) record.get("remote_service_name");
        if (parent != null && !parent.isEmpty() && child != null && !child.isEmpty()) {
          links.add(DependencyLink.newBuilder()
            .parent(parent)
            .child(child)
            .callCount(1)
            .build());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read dependencies from ClickHouse", e);
    }

    return links;
  }

  private static Long getLong(Object value) {
    if (value == null) return null;
    if (value instanceof Long) return (Long) value;
    if (value instanceof Integer) return ((Integer) value).longValue();
    if (value instanceof BigInteger) return ((BigInteger) value).longValue();
    if (value instanceof ZonedDateTime) {
      Instant instant = ((ZonedDateTime) value).toInstant();
      return instant.toEpochMilli() * 1000 + instant.getNano() / 1000;
    }
    if (value instanceof String) {
      try {
        return Long.parseLong((String) value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private static Integer getInteger(Object value) {
    if (value == null) return null;
    if (value instanceof Integer) return (Integer) value;
    if (value instanceof Long) return ((Long) value).intValue();
    if (value instanceof String) {
      try {
        return Integer.parseInt((String) value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
