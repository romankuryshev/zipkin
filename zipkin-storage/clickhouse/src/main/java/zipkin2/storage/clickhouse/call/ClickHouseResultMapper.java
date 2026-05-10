package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.SpanStatistics;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

public final class ClickHouseResultMapper {

  private ClickHouseResultMapper() {}

  static String getStatisticsJoinFragment(String database, String serviceName) {
    StringBuilder sb = new StringBuilder();
    sb.append(" LEFT JOIN (SELECT span_name, span_kind, service_name, ")
      .append("medianMerge(median_duration) AS median_duration, ")
      .append("avgMerge(average_duration) AS average_duration, ")
      .append("quantileMerge(p50) AS p50, ")
      .append("quantileMerge(p95) AS p95, ")
      .append("quantileMerge(p99) AS p99, ")
      .append("sumMerge(success_count) AS success_count, ")
      .append("sumMerge(error_count) AS error_count, ")
      .append("sumMerge(total_count) AS total_count ")
      .append("FROM ").append(database).append(".spans_aggregate_stats");
    if (serviceName != null) {
      sb.append(" WHERE service_name = {statsServiceName:String}");
    }
    sb.append(" GROUP BY span_name, span_kind, service_name) AS stats ")
      .append("ON s.name = stats.span_name AND s.kind = stats.span_kind ")
      .append("AND s.local_endpoint_service_name = stats.service_name");
    return sb.toString();
  }

  static Span toSpan(Map<String, Object> record) {
    return toSpan(record, true);
  }

  // Reads only stats columns when includeSpanStatistics=true.
  // Reading absent columns triggers NoSuchColumnException inside RecordWrapper.get()
  // which is caught and turned into null — but fillInStackTrace dominates CPU under load.
  static Span toSpan(Map<String, Object> record, boolean includeSpanStatistics) {
    Span.Builder builder = Span.newBuilder();

    BigInteger traceIdLow = getBigInteger(record.get("trace_id"));
    BigInteger traceIdHigh = getBigInteger(record.get("trace_id_high"));
    String traceId = combineTraceId(traceIdLow, traceIdHigh);
    builder.traceId(traceId);

    BigInteger spanIdBig = getBigInteger(record.get("span_id"));
    builder.id(spanIdBig != null ? Long.toHexString(spanIdBig.longValue()) : "0");

    BigInteger parentIdBig = getBigInteger(record.get("parent_id"));
    if (parentIdBig != null && parentIdBig.signum() > 0) {
      builder.parentId(Long.toHexString(parentIdBig.longValue()));
    }

    builder.name((String) record.get("name"));

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

    if (includeSpanStatistics) {
      BigDecimal medianDuration = getBigDecimal(record.get("median_duration"));
      BigDecimal averageDuration = getBigDecimal(record.get("average_duration"));
      BigDecimal p50 = getBigDecimal(record.get("p50"));
      BigDecimal p95 = getBigDecimal(record.get("p95"));
      BigDecimal p99 = getBigDecimal(record.get("p99"));
      Long successCount = getLong(record.get("success_count"));
      Long errorCount = getLong(record.get("error_count"));
      Long totalCount = getLong(record.get("total_count"));

      if (medianDuration != null || averageDuration != null || p50 != null || p95 != null ||
          p99 != null || successCount != null || errorCount != null || totalCount != null) {
        String spanName = (String) record.get("name");
        SpanStatistics stats = new SpanStatistics(
          spanName,
          spanKind != null ? spanKind : "",
          medianDuration != null ? medianDuration : BigDecimal.ZERO,
          averageDuration != null ? averageDuration : BigDecimal.ZERO,
          p50 != null ? p50 : BigDecimal.ZERO,
          p95 != null ? p95 : BigDecimal.ZERO,
          p99 != null ? p99 : BigDecimal.ZERO,
          successCount != null ? successCount : 0L,
          errorCount != null ? errorCount : 0L,
          totalCount != null ? totalCount : 0L
        );
        builder.statistics(stats);
      }
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
        if (annObj instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, Object> annMap = (Map<String, Object>) annObj;
          Long annTimestamp = getLong(annMap.get("timestamp"));
          String annValue = (String) annMap.get("value");
          if (annTimestamp != null && annValue != null) {
            builder.addAnnotation(annTimestamp, annValue);
          }
        } else if (annObj instanceof List) {
          @SuppressWarnings("unchecked")
          List<Object> annList = (List<Object>) annObj;
          if (annList.size() >= 2) {
            Long annTimestamp = getLong(annList.get(0));
            Object val = annList.get(1);
            String annValue = val instanceof String ? (String) val : null;
            if (annTimestamp != null && annValue != null) {
              builder.addAnnotation(annTimestamp, annValue);
            }
          }
        } else if (annObj instanceof Object[]) {
          Object[] annArr = (Object[]) annObj;
          if (annArr.length >= 2) {
            Long annTimestamp = getLong(annArr[0]);
            String annValue = annArr[1] instanceof String ? (String) annArr[1] : null;
            if (annTimestamp != null && annValue != null) {
              builder.addAnnotation(annTimestamp, annValue);
            }
          }
        }
      }
    }

    Object sharedVal = record.get("shared");
    if (sharedVal != null) {
      int s = sharedVal instanceof Number ? ((Number) sharedVal).intValue() : 0;
      if (s == 1) builder.shared(true);
    }

    Object debugVal = record.get("debug");
    if (debugVal != null) {
      int d = debugVal instanceof Number ? ((Number) debugVal).intValue() : 0;
      if (d == 1) builder.debug(true);
    }

    return builder.build();
  }

  static List<Span> toSpans(QueryResponse response, Client client) {
    return toSpans(response, client, true);
  }

  static List<Span> toSpans(QueryResponse response, Client client, boolean includeSpanStatistics) {
    List<Span> spans = new ArrayList<>();

    try (ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
      while (reader.hasNext()) {
        Map<String, Object> record = reader.next();
        spans.add(toSpan(record, includeSpanStatistics));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read spans from ClickHouse", e);
    }

    return spans;
  }

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
        String child = (String) record.get("local_service_name");
        String parent = (String) record.get("remote_service_name");
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

  private static String combineTraceId(BigInteger traceIdLow, BigInteger traceIdHigh) {
    if (traceIdLow == null) traceIdLow = BigInteger.ZERO;
    if (traceIdHigh == null) traceIdHigh = BigInteger.ZERO;

    // longValue() extracts the raw 64 bits — correct for UInt64 hex representation.
    // String.format("%016x") is a pure bit-shift operation, ~10x faster than BigInteger.toString(16).
    String lowHex = String.format("%016x", traceIdLow.longValue());

    if (traceIdHigh.signum() == 0) {
      return lowHex;
    }

    return String.format("%016x", traceIdHigh.longValue()) + lowHex;
  }

  private static Long getLong(Object value) {
    if (value == null) return null;
    if (value instanceof Long) return (Long) value;
    if (value instanceof Integer) return ((Integer) value).longValue();
    if (value instanceof BigInteger) return ((BigInteger) value).longValue();
    if (value instanceof ZonedDateTime) {
      Instant instant = ((ZonedDateTime) value).toInstant();
      return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
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

  private static BigInteger getBigInteger(Object value) {
    if (value == null) return null;
    if (value instanceof BigInteger) return (BigInteger) value;
    if (value instanceof Long) return BigInteger.valueOf((Long) value);
    if (value instanceof Integer) return BigInteger.valueOf((Integer) value);
    if (value instanceof String) {
      try {
        return new BigInteger((String) value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private static BigDecimal getBigDecimal(Object value) {
    if (value == null) return null;
    if (value instanceof BigDecimal) return (BigDecimal) value;
    if (value instanceof Double) return BigDecimal.valueOf((Double) value);
    if (value instanceof Float) return BigDecimal.valueOf((Float) value);
    if (value instanceof Long) return BigDecimal.valueOf((Long) value);
    if (value instanceof Integer) return BigDecimal.valueOf((Integer) value);
    if (value instanceof BigInteger) return new BigDecimal((BigInteger) value);
    if (value instanceof String) {
      try {
        return new BigDecimal((String) value);
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

