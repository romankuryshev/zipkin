package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.GenericRecord;
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

public final class ClickHouseResultMapper {

  private ClickHouseResultMapper() {}

  // Lookup table for hex encoding — no String.format, no varargs, no boxing.
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private static String toHex16(long v) {
    char[] buf = new char[16];
    for (int i = 15; i >= 0; i--) {
      buf[i] = HEX[(int) (v & 0xF)];
      v >>>= 4;
    }
    return new String(buf);
  }

  /**
   * Reads a UInt64 column as a Java long (raw 64-bit two's complement).
   * reader.getLong() throws ArithmeticException for values > Long.MAX_VALUE,
   * so we go through readValue() which returns BigInteger, then take the low 64 bits.
   * This is safe for hex encoding: the bit pattern is identical.
   */
  private static long readUInt64(ClickHouseBinaryFormatReader reader, String col) {
    Object v = reader.readValue(col);
    if (v instanceof Long) return (Long) v;
    if (v instanceof BigInteger) return ((BigInteger) v).longValue();
    return 0L;
  }

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

  // ── hot path ────────────────────────────────────────────────────────────────

  /**
   * Reads the current row from the typed reader.
   * Called after reader.next() has advanced the cursor.
   * Uses typed getters (getLong, getString, getInstant, …) instead of the
   * Map<String,Object> returned by next() — eliminates HashMap lookup overhead
   * and avoids BigInteger creation for UInt64 columns where possible.
   */
  private static Span toSpanFromReader(ClickHouseBinaryFormatReader reader,
                                       boolean includeSpanStatistics) {
    Span.Builder builder = Span.newBuilder();

    // UInt64 — readUInt64() extracts raw 64 bits without ArithmeticException.
    long traceIdLow  = readUInt64(reader, "trace_id");
    long traceIdHigh = readUInt64(reader, "trace_id_high");
    builder.traceId(traceIdHigh == 0L ? toHex16(traceIdLow)
                                      : toHex16(traceIdHigh) + toHex16(traceIdLow));

    builder.id(Long.toHexString(readUInt64(reader, "span_id")));

    if (reader.hasValue("parent_id")) {
      long parentId = readUInt64(reader, "parent_id");
      if (parentId != 0L) builder.parentId(Long.toHexString(parentId));
    }

    String spanName = reader.getString("name");
    builder.name(spanName);

    // Local endpoint
    String localSvc = reader.getString("local_endpoint_service_name");
    Inet4Address localIpv4 = reader.hasValue("local_endpoint_ipv4")
        ? reader.getInet4Address("local_endpoint_ipv4") : null;
    Inet6Address localIpv6 = reader.hasValue("local_endpoint_ipv6")
        ? reader.getInet6Address("local_endpoint_ipv6") : null;
    Integer localPort = reader.hasValue("local_endpoint_port")
        ? reader.getInteger("local_endpoint_port") : null;
    if (localSvc != null || localIpv4 != null || localIpv6 != null || localPort != null) {
      Endpoint.Builder ep = Endpoint.newBuilder();
      if (localSvc != null && !localSvc.isEmpty()) ep.serviceName(localSvc);
      if (localIpv4 != null) ep.parseIp(localIpv4);
      if (localIpv6 != null) ep.parseIp(localIpv6);
      if (localPort != null) ep.port(localPort);
      builder.localEndpoint(ep.build());
    }

    // Remote endpoint
    String remoteSvc = reader.getString("remote_endpoint_service_name");
    Inet4Address remoteIpv4 = reader.hasValue("remote_endpoint_ipv4")
        ? reader.getInet4Address("remote_endpoint_ipv4") : null;
    Inet6Address remoteIpv6 = reader.hasValue("remote_endpoint_ipv6")
        ? reader.getInet6Address("remote_endpoint_ipv6") : null;
    Integer remotePort = reader.hasValue("remote_endpoint_port")
        ? reader.getInteger("remote_endpoint_port") : null;
    if (remoteSvc != null || remoteIpv4 != null || remoteIpv6 != null || remotePort != null) {
      Endpoint.Builder ep = Endpoint.newBuilder();
      if (remoteSvc != null && !remoteSvc.isEmpty()) ep.serviceName(remoteSvc);
      if (remoteIpv4 != null) ep.parseIp(remoteIpv4);
      if (remoteIpv6 != null) ep.parseIp(remoteIpv6);
      if (remotePort != null) ep.port(remotePort);
      builder.remoteEndpoint(ep.build());
    }

    String spanKind = reader.getString("kind");
    if (spanKind != null && !spanKind.isEmpty()) {
      try {
        builder.kind(Span.Kind.valueOf(spanKind));
      } catch (IllegalArgumentException ignored) {
      }
    }

    // Instant is cheaper than ZonedDateTime: no timezone object created.
    Instant ts = reader.getInstant("timestamp");
    if (ts != null) {
      long tsMicros = ts.getEpochSecond() * 1_000_000L + ts.getNano() / 1_000L;
      if (tsMicros > 0) builder.timestamp(tsMicros);
    }

    long duration = readUInt64(reader, "duration");
    if (duration > 0) builder.duration(duration);

    if (includeSpanStatistics) {
      BigDecimal medianDuration  = reader.hasValue("median_duration")  ? reader.getBigDecimal("median_duration")  : null;
      BigDecimal averageDuration = reader.hasValue("average_duration") ? reader.getBigDecimal("average_duration") : null;
      BigDecimal p50             = reader.hasValue("p50")              ? reader.getBigDecimal("p50")              : null;
      BigDecimal p95             = reader.hasValue("p95")              ? reader.getBigDecimal("p95")              : null;
      BigDecimal p99             = reader.hasValue("p99")              ? reader.getBigDecimal("p99")              : null;
      Long successCount = reader.hasValue("success_count") ? readUInt64(reader, "success_count") : null;
      Long errorCount   = reader.hasValue("error_count")   ? readUInt64(reader, "error_count")   : null;
      Long totalCount   = reader.hasValue("total_count")   ? readUInt64(reader, "total_count")   : null;

      if (medianDuration != null || averageDuration != null || p50 != null || p95 != null ||
          p99 != null || successCount != null || errorCount != null || totalCount != null) {
        builder.statistics(new SpanStatistics(
          spanName,
          spanKind != null ? spanKind : "",
          medianDuration  != null ? medianDuration  : BigDecimal.ZERO,
          averageDuration != null ? averageDuration : BigDecimal.ZERO,
          p50 != null ? p50 : BigDecimal.ZERO,
          p95 != null ? p95 : BigDecimal.ZERO,
          p99 != null ? p99 : BigDecimal.ZERO,
          successCount != null ? successCount : 0L,
          errorCount   != null ? errorCount   : 0L,
          totalCount   != null ? totalCount   : 0L
        ));
      }
    }

    @SuppressWarnings("unchecked")
    Map<String, String> tags = reader.readValue("tags");
    if (tags != null) {
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        builder.putTag(tag.getKey(), tag.getValue());
      }
    }

    List<Object> annList = reader.getList("annotations");
    if (annList != null && !annList.isEmpty()) {
      readAnnotations(builder, annList);
    }

    if (reader.getByte("shared") == 1) builder.shared(true);
    if (reader.getByte("debug") == 1) builder.debug(true);

    return builder.build();
  }

  // ── public API ──────────────────────────────────────────────────────────────

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
    builder.traceId(combineTraceId(traceIdLow, traceIdHigh));

    BigInteger spanIdBig = getBigInteger(record.get("span_id"));
    builder.id(spanIdBig != null ? Long.toHexString(spanIdBig.longValue()) : "0");

    BigInteger parentIdBig = getBigInteger(record.get("parent_id"));
    if (parentIdBig != null && parentIdBig.signum() > 0) {
      builder.parentId(Long.toHexString(parentIdBig.longValue()));
    }

    builder.name((String) record.get("name"));

    String localServiceName = (String) record.get("local_endpoint_service_name");
    Inet4Address localIpv4  = (Inet4Address) record.get("local_endpoint_ipv4");
    Inet6Address localIpv6  = (Inet6Address) record.get("local_endpoint_ipv6");
    Integer localPort       = getInteger(record.get("local_endpoint_port"));
    if (localServiceName != null || localIpv4 != null || localIpv6 != null || localPort != null) {
      Endpoint.Builder ep = Endpoint.newBuilder();
      if (localServiceName != null && !localServiceName.isEmpty()) ep.serviceName(localServiceName);
      if (localIpv4 != null) ep.parseIp(localIpv4);
      if (localIpv6 != null) ep.parseIp(localIpv6);
      if (localPort != null) ep.port(localPort);
      builder.localEndpoint(ep.build());
    }

    String remoteServiceName = (String) record.get("remote_endpoint_service_name");
    Inet4Address remoteIpv4  = (Inet4Address) record.get("remote_endpoint_ipv4");
    Inet6Address remoteIpv6  = (Inet6Address) record.get("remote_endpoint_ipv6");
    Integer remotePort       = getInteger(record.get("remote_endpoint_port"));
    if (remoteServiceName != null || remoteIpv4 != null || remoteIpv6 != null || remotePort != null) {
      Endpoint.Builder ep = Endpoint.newBuilder();
      if (remoteServiceName != null && !remoteServiceName.isEmpty()) ep.serviceName(remoteServiceName);
      if (remoteIpv4 != null) ep.parseIp(remoteIpv4);
      if (remoteIpv6 != null) ep.parseIp(remoteIpv6);
      if (remotePort != null) ep.port(remotePort);
      builder.remoteEndpoint(ep.build());
    }

    String spanKind = (String) record.get("kind");
    if (spanKind != null && !spanKind.isEmpty()) {
      try {
        builder.kind(Span.Kind.valueOf(spanKind));
      } catch (IllegalArgumentException ignored) {
      }
    }

    Long timestamp = getLong(record.get("timestamp"));
    if (timestamp != null && timestamp > 0) builder.timestamp(timestamp);

    Long duration = getLong(record.get("duration"));
    if (duration != null && duration > 0) builder.duration(duration);

    if (includeSpanStatistics) {
      BigDecimal medianDuration  = getBigDecimal(record.get("median_duration"));
      BigDecimal averageDuration = getBigDecimal(record.get("average_duration"));
      BigDecimal p50 = getBigDecimal(record.get("p50"));
      BigDecimal p95 = getBigDecimal(record.get("p95"));
      BigDecimal p99 = getBigDecimal(record.get("p99"));
      Long successCount = getLong(record.get("success_count"));
      Long errorCount   = getLong(record.get("error_count"));
      Long totalCount   = getLong(record.get("total_count"));

      if (medianDuration != null || averageDuration != null || p50 != null || p95 != null ||
          p99 != null || successCount != null || errorCount != null || totalCount != null) {
        String spanName = (String) record.get("name");
        builder.statistics(new SpanStatistics(
          spanName,
          spanKind != null ? spanKind : "",
          medianDuration  != null ? medianDuration  : BigDecimal.ZERO,
          averageDuration != null ? averageDuration : BigDecimal.ZERO,
          p50 != null ? p50 : BigDecimal.ZERO,
          p95 != null ? p95 : BigDecimal.ZERO,
          p99 != null ? p99 : BigDecimal.ZERO,
          successCount != null ? successCount : 0L,
          errorCount   != null ? errorCount   : 0L,
          totalCount   != null ? totalCount   : 0L
        ));
      }
    }

    @SuppressWarnings("unchecked")
    Map<String, String> tags = (Map<String, String>) record.get("tags");
    if (tags != null) {
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        builder.putTag(tag.getKey(), tag.getValue());
      }
    }

    Object annRaw = record.get("annotations");
    if (annRaw instanceof BinaryStreamReader.ArrayValue) {
      List<Object> annList = ((BinaryStreamReader.ArrayValue) annRaw).asList();
      if (annList != null && !annList.isEmpty()) readAnnotations(builder, annList);
    }

    Object sharedVal = record.get("shared");
    if (sharedVal instanceof Number && ((Number) sharedVal).intValue() == 1) builder.shared(true);
    Object debugVal = record.get("debug");
    if (debugVal instanceof Number && ((Number) debugVal).intValue() == 1) builder.debug(true);

    return builder.build();
  }

  // ── GenericRecord path (used by queryAll) ───────────────────────────────────
static List<BigInteger> toTraceIds(List<GenericRecord> rows) {
    return rows.stream()
      .map(row -> row.getBigInteger("trace_id"))
      .toList();
}

  static List<Span> toSpans(List<GenericRecord> rows, boolean includeSpanStatistics) {
    List<Span> spans = new ArrayList<>(rows.size());
    for (GenericRecord row : rows) {
      spans.add(toSpanFromGenericRecord(row, includeSpanStatistics));
    }
    return spans;
  }

  private static long readUInt64(GenericRecord row, String col) {
    BigInteger v = row.getBigInteger(col);
    return v != null ? v.longValue() : 0L;
  }

  private static long readUInt64Nullable(GenericRecord row, String col) {
    if (!row.hasValue(col)) return 0L;
    BigInteger v = row.getBigInteger(col);
    return v != null ? v.longValue() : 0L;
  }

  private static Span toSpanFromGenericRecord(GenericRecord row, boolean includeSpanStatistics) {
    Span.Builder builder = Span.newBuilder();

    long traceIdLow  = readUInt64(row, "trace_id");
    long traceIdHigh = readUInt64(row, "trace_id_high");
    builder.traceId(traceIdHigh == 0L ? toHex16(traceIdLow)
                                      : toHex16(traceIdHigh) + toHex16(traceIdLow));

    builder.id(Long.toHexString(readUInt64(row, "span_id")));

    long parentId = readUInt64Nullable(row, "parent_id");
    if (parentId != 0L) builder.parentId(Long.toHexString(parentId));

    String spanName = row.getString("name");
    builder.name(spanName);

    String localSvc  = row.getString("local_endpoint_service_name");
    Inet4Address localIpv4 = row.hasValue("local_endpoint_ipv4") ? row.getInet4Address("local_endpoint_ipv4") : null;
    Inet6Address localIpv6 = row.hasValue("local_endpoint_ipv6") ? row.getInet6Address("local_endpoint_ipv6") : null;
    Integer localPort      = row.hasValue("local_endpoint_port")  ? row.getInteger("local_endpoint_port")      : null;
    if (localSvc != null || localIpv4 != null || localIpv6 != null || localPort != null) {
      Endpoint.Builder ep = Endpoint.newBuilder();
      if (localSvc != null && !localSvc.isEmpty()) ep.serviceName(localSvc);
      if (localIpv4 != null) ep.parseIp(localIpv4);
      if (localIpv6 != null) ep.parseIp(localIpv6);
      if (localPort != null) ep.port(localPort);
      builder.localEndpoint(ep.build());
    }

    String remoteSvc  = row.getString("remote_endpoint_service_name");
    Inet4Address remoteIpv4 = row.hasValue("remote_endpoint_ipv4") ? row.getInet4Address("remote_endpoint_ipv4") : null;
    Inet6Address remoteIpv6 = row.hasValue("remote_endpoint_ipv6") ? row.getInet6Address("remote_endpoint_ipv6") : null;
    Integer remotePort      = row.hasValue("remote_endpoint_port")  ? row.getInteger("remote_endpoint_port")      : null;
    if (remoteSvc != null || remoteIpv4 != null || remoteIpv6 != null || remotePort != null) {
      Endpoint.Builder ep = Endpoint.newBuilder();
      if (remoteSvc != null && !remoteSvc.isEmpty()) ep.serviceName(remoteSvc);
      if (remoteIpv4 != null) ep.parseIp(remoteIpv4);
      if (remoteIpv6 != null) ep.parseIp(remoteIpv6);
      if (remotePort != null) ep.port(remotePort);
      builder.remoteEndpoint(ep.build());
    }

    String spanKind = row.getString("kind");
    if (spanKind != null && !spanKind.isEmpty()) {
      try { builder.kind(Span.Kind.valueOf(spanKind)); } catch (IllegalArgumentException ignored) {}
    }

    ZonedDateTime ts = row.getZonedDateTime("timestamp");
    if (ts != null) {
      Instant tsi = ts.toInstant();
      long tsMicros = tsi.getEpochSecond() * 1_000_000L + tsi.getNano() / 1_000L;
      if (tsMicros > 0) builder.timestamp(tsMicros);
    }

    long duration = readUInt64(row, "duration");
    if (duration > 0) builder.duration(duration);

    if (includeSpanStatistics) {
      BigDecimal medianDuration  = row.hasValue("median_duration")  ? row.getBigDecimal("median_duration")  : null;
      BigDecimal averageDuration = row.hasValue("average_duration") ? row.getBigDecimal("average_duration") : null;
      BigDecimal p50 = row.hasValue("p50") ? row.getBigDecimal("p50") : null;
      BigDecimal p95 = row.hasValue("p95") ? row.getBigDecimal("p95") : null;
      BigDecimal p99 = row.hasValue("p99") ? row.getBigDecimal("p99") : null;
      Long successCount = row.hasValue("success_count") ? readUInt64(row, "success_count") : null;
      Long errorCount   = row.hasValue("error_count")   ? readUInt64(row, "error_count")   : null;
      Long totalCount   = row.hasValue("total_count")   ? readUInt64(row, "total_count")   : null;
      if (medianDuration != null || averageDuration != null || p50 != null || p95 != null ||
          p99 != null || successCount != null || errorCount != null || totalCount != null) {
        builder.statistics(new SpanStatistics(
          spanName, spanKind != null ? spanKind : "",
          medianDuration  != null ? medianDuration  : BigDecimal.ZERO,
          averageDuration != null ? averageDuration : BigDecimal.ZERO,
          p50 != null ? p50 : BigDecimal.ZERO,
          p95 != null ? p95 : BigDecimal.ZERO,
          p99 != null ? p99 : BigDecimal.ZERO,
          successCount != null ? successCount : 0L,
          errorCount   != null ? errorCount   : 0L,
          totalCount   != null ? totalCount   : 0L
        ));
      }
    }

    @SuppressWarnings("unchecked")
    Map<String, String> tags = (Map<String, String>) row.getObject("tags");
    if (tags != null) {
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        builder.putTag(tag.getKey(), tag.getValue());
      }
    }

    List<Object> annList = row.getList("annotations");
    if (annList != null && !annList.isEmpty()) readAnnotations(builder, annList);

    if (row.getByte("shared") == 1) builder.shared(true);
    if (row.getByte("debug") == 1) builder.debug(true);

    return builder.build();
  }

  // ── QueryResponse / BinaryFormatReader path (used by GetTraceCall, GetTracesByIdCall) ──

  static List<Span> toSpans(QueryResponse response, Client client) {
    return toSpans(response, client, true);
  }

  static List<Span> toSpans(QueryResponse response, Client client, boolean includeSpanStatistics) {
    List<Span> spans = new ArrayList<>();
    try (ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
      while (reader.hasNext()) {
        reader.next(); // advance cursor; discard Map wrapper — use typed getters below
        spans.add(toSpanFromReader(reader, includeSpanStatistics));
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
        reader.next();
        String value = reader.getString(columnName);
        if (value != null && !value.isEmpty()) result.add(value);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read strings from ClickHouse", e);
    }
    return result;
  }

  static List<List<Span>> groupSpansByTraceId(List<Span> spans) {
    Map<String, List<Span>> grouped = new LinkedHashMap<>();
    for (Span span : spans) {
      grouped.computeIfAbsent(span.traceId(), k -> new ArrayList<>()).add(span);
    }
    return new ArrayList<>(grouped.values());
  }

  static List<DependencyLink> toDependencyLinks(QueryResponse response, Client client) {
    List<DependencyLink> links = new ArrayList<>();
    try (ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
      while (reader.hasNext()) {
        reader.next();
        String child  = reader.getString("local_service_name");
        String parent = reader.getString("remote_service_name");
        if (parent != null && !parent.isEmpty() && child != null && !child.isEmpty()) {
          links.add(DependencyLink.newBuilder().parent(parent).child(child).callCount(1).build());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read dependencies from ClickHouse", e);
    }
    return links;
  }

  // ── helpers ─────────────────────────────────────────────────────────────────

  private static void readAnnotations(Span.Builder builder, List<Object> annList) {
    for (Object annObj : annList) {
      if (annObj instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> annMap = (Map<String, Object>) annObj;
        Long annTs = getLong(annMap.get("timestamp"));
        String annVal = (String) annMap.get("value");
        if (annTs != null && annVal != null) builder.addAnnotation(annTs, annVal);
      } else if (annObj instanceof List) {
        @SuppressWarnings("unchecked")
        List<Object> annL = (List<Object>) annObj;
        if (annL.size() >= 2) {
          Long annTs = getLong(annL.get(0));
          Object v = annL.get(1);
          if (annTs != null && v instanceof String) builder.addAnnotation(annTs, (String) v);
        }
      } else if (annObj instanceof Object[]) {
        Object[] annArr = (Object[]) annObj;
        if (annArr.length >= 2) {
          Long annTs = getLong(annArr[0]);
          if (annTs != null && annArr[1] instanceof String) builder.addAnnotation(annTs, (String) annArr[1]);
        }
      }
    }
  }

  private static String combineTraceId(BigInteger low, BigInteger high) {
    if (low == null) low = BigInteger.ZERO;
    if (high == null) high = BigInteger.ZERO;
    return high.signum() == 0
        ? toHex16(low.longValue())
        : toHex16(high.longValue()) + toHex16(low.longValue());
  }

  private static Long getLong(Object value) {
    if (value == null) return null;
    if (value instanceof Long) return (Long) value;
    if (value instanceof Integer) return ((Integer) value).longValue();
    if (value instanceof BigInteger) return ((BigInteger) value).longValue();
    if (value instanceof Instant) {
      Instant i = (Instant) value;
      return i.getEpochSecond() * 1_000_000L + i.getNano() / 1_000L;
    }
    if (value instanceof ZonedDateTime) {
      Instant i = ((ZonedDateTime) value).toInstant();
      return i.getEpochSecond() * 1_000_000L + i.getNano() / 1_000L;
    }
    if (value instanceof String) {
      try { return Long.parseLong((String) value); } catch (NumberFormatException e) { return null; }
    }
    return null;
  }

  private static BigInteger getBigInteger(Object value) {
    if (value == null) return null;
    if (value instanceof BigInteger) return (BigInteger) value;
    if (value instanceof Long) return BigInteger.valueOf((Long) value);
    if (value instanceof Integer) return BigInteger.valueOf((Integer) value);
    if (value instanceof String) {
      try { return new BigInteger((String) value); } catch (NumberFormatException e) { return null; }
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
      try { return new BigDecimal((String) value); } catch (NumberFormatException e) { return null; }
    }
    return null;
  }

  private static Integer getInteger(Object value) {
    if (value == null) return null;
    if (value instanceof Integer) return (Integer) value;
    if (value instanceof Long) return ((Long) value).intValue();
    if (value instanceof String) {
      try { return Integer.parseInt((String) value); } catch (NumberFormatException e) { return null; }
    }
    return null;
  }
}
