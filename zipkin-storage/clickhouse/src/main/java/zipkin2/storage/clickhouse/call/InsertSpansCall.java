package zipkin2.storage.clickhouse.call;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.clickhouse.cache.AutocompleteTagsCache;
import zipkin2.storage.clickhouse.dto.DependencyRecord;
import zipkin2.storage.clickhouse.dto.ServiceOperationNameRecord;
import zipkin2.storage.clickhouse.dto.SpanRecord;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ExecutionException;

public final class InsertSpansCall extends Call<Void> {
  private final Client client;
  private final List<Span> spans;
  private final boolean strictTraceId;
  private final Set<String> autocompleteKeys;
  private final AutocompleteTagsCache autocompleteTagsCache;

  public InsertSpansCall(Client client, List<Span> spans, boolean strictTraceId,
                         Set<String> autocompleteKeys,
                         AutocompleteTagsCache autocompleteTagsCache) {
    this.client = client;
    this.spans = spans;
    this.strictTraceId = strictTraceId;
    this.autocompleteKeys = autocompleteKeys;
    this.autocompleteTagsCache = autocompleteTagsCache;
  }

  @Override
  public Void execute() {
    try {
      // 1. Convert Spans to SpanRecords and insert into spans table
      List<SpanRecord> spanRecords = convertToSpanRecords();
      if (!spanRecords.isEmpty()) {
        client.insert("spans", spanRecords).get();
      }

      // 2. Extract and insert into service_operation_names table
      List<ServiceOperationNameRecord> serviceOpsRecords = new ArrayList<>(extractServiceOperationNames());
      if (!serviceOpsRecords.isEmpty()) {
        client.insert("service_operation_names", serviceOpsRecords).get();
      }

      // 3. Extract and insert into dependencies table
      List<DependencyRecord> depsRecords = new ArrayList<>(extractDependencies());
      if (!depsRecords.isEmpty()) {
        client.insert("dependencies", depsRecords).get();
      }

      // 4. Insert autocomplete tag values if configured
      if (!autocompleteKeys.isEmpty()) {
        insertAutocompleteData();
      }
    } catch (InterruptedException | ExecutionException e) {
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

  private List<SpanRecord> convertToSpanRecords() {
    List<SpanRecord> records = new ArrayList<>();

    for (Span span : spans) {
      boolean traceIdHigh = !strictTraceId && span.traceId().length() == 32;
      String traceIdToUse = traceIdHigh ? span.traceId().substring(16) : span.traceId();
      String traceIdHighStr = traceIdHigh ? span.traceId().substring(0, 16) : null;

      BigInteger traceIdLow = parseHexStringToBigInteger(traceIdToUse);
      BigInteger traceIdHighVal = traceIdHighStr != null ? parseHexStringToBigInteger(traceIdHighStr) : BigInteger.ZERO;
      BigInteger parentIdVal = span.parentId() != null && !span.parentId().isEmpty()
        ? parseHexStringToBigInteger(span.parentId())
        : null;
      BigInteger spanId = parseHexStringToBigInteger(span.id());
      List<Object[]> annotations = buildAnnotationsList(span);

      String statusCode = StatusCodeResolver.resolveStatusCode(span);
      SpanRecord record = new SpanRecord(
        traceIdLow,
        traceIdHighVal,
        parentIdVal,
        spanId,
        span.kind() != null ? span.kind().toString() : "",
        span.name() != null ? span.name() : "",
        convertTimestampToInstant(span.timestampAsLong()),
        span.durationAsLong() > 0 ? span.durationAsLong() : 0,
        getLocalEndpointServiceName(span),
        convertToInet4Address(span.localEndpoint() != null ? span.localEndpoint().ipv4() : null),
        convertToInet6Address(span.localEndpoint() != null ? span.localEndpoint().ipv6() : null),
        span.localEndpoint() != null ? span.localEndpoint().port() : null,
        getRemoteEndpointServiceName(span),
        convertToInet4Address(span.remoteEndpoint() != null ? span.remoteEndpoint().ipv4() : null),
        convertToInet6Address(span.remoteEndpoint() != null ? span.remoteEndpoint().ipv6() : null),
        span.remoteEndpoint() != null ? span.remoteEndpoint().port() : null,
        annotations,
        span.tags(),
        statusCode
      );

      records.add(record);
    }

    return records;
  }

  private Set<ServiceOperationNameRecord> extractServiceOperationNames() {
    Set<ServiceOperationNameRecord> records = new HashSet<>();

    for (Span span : spans) {
      String serviceName = getServiceName(span);
      String operationName = span.name();

      if (serviceName != null && !serviceName.isEmpty() &&
          operationName != null && !operationName.isEmpty()) {
        records.add(new ServiceOperationNameRecord(serviceName, operationName));
      }
    }

    return records;
  }

  private Set<DependencyRecord> extractDependencies() {
    Set<DependencyRecord> records = new HashSet<>();

    for (Span span : spans) {
      String localService = getServiceName(span);
      String remoteService = null;

      if (span.remoteEndpoint() != null && span.remoteEndpoint().serviceName() != null) {
        remoteService = span.remoteEndpoint().serviceName();
      }

      if (localService != null && !localService.isEmpty() &&
          remoteService != null && !remoteService.isEmpty()) {
        records.add(new DependencyRecord(localService, remoteService));
      }
    }

    return records;
  }

  private BigInteger parseHexStringToBigInteger(String hexStr) {
    if (hexStr == null || hexStr.isEmpty()) return BigInteger.ZERO;
    try {
      return new BigInteger(hexStr, 16);
    } catch (Exception e) {
      return BigInteger.ZERO;
    }
  }

  private List<Object[]> buildAnnotationsList(Span span) {
    if (span.annotations() == null || span.annotations().isEmpty()) {
      return Collections.emptyList();
    }

    List<Object[]> annotations = new ArrayList<>();
    for (zipkin2.Annotation annotation : span.annotations()) {
      Object[] tuple = new Object[2];
      tuple[0] = convertTimestampToInstant(annotation.timestamp());
      tuple[1] = annotation.value();
      annotations.add(tuple);
    }
    return annotations;
  }

  private String getLocalEndpointServiceName(Span span) {
    if (span.localEndpoint() != null && span.localEndpoint().serviceName() != null) {
      return span.localEndpoint().serviceName();
    }
    return "";
  }

  private String getRemoteEndpointServiceName(Span span) {
    if (span.remoteEndpoint() != null && span.remoteEndpoint().serviceName() != null) {
      return span.remoteEndpoint().serviceName();
    }
    return "";
  }

  private String getServiceName(Span span) {
    if (span.localEndpoint() != null && span.localEndpoint().serviceName() != null) {
      return span.localEndpoint().serviceName();
    }
    return span.tags().get("service");
  }

  private java.time.Instant convertTimestampToInstant(long timestampMicros) {
    if (timestampMicros <= 0) {
      return java.time.Instant.EPOCH;
    }
    // Конвертируем микросекунды в секунды и наносекунды
    long seconds = timestampMicros / 1_000_000;
    long nanos = (timestampMicros % 1_000_000) * 1_000;
    return java.time.Instant.ofEpochSecond(seconds, nanos);
  }

  private java.net.Inet4Address convertToInet4Address(String ipv4String) {
    if (ipv4String == null || ipv4String.isEmpty()) {
      return null;
    }
    try {
      return (java.net.Inet4Address) java.net.InetAddress.getByName(ipv4String);
    } catch (Exception e) {
      return null;
    }
  }

  private java.net.Inet6Address convertToInet6Address(String ipv6String) {
    if (ipv6String == null || ipv6String.isEmpty()) {
      return null;
    }
    try {
      return (java.net.Inet6Address) java.net.InetAddress.getByName(ipv6String);
    } catch (Exception e) {
      return null;
    }
  }

  private void insertAutocompleteData() {
    if (autocompleteTagsCache == null || autocompleteKeys.isEmpty()) {
      return;
    }

    Map<String, Set<String>> tagValuesByKey = new HashMap<>();

    for (Span span : spans) {
      if (span.tags() == null || span.tags().isEmpty()) continue;

      for (String key : autocompleteKeys) {
        String value = span.tags().get(key);
        if (value != null && !value.isEmpty()) {
          tagValuesByKey.computeIfAbsent(key, k -> new HashSet<>())
            .add(value);
        }
      }
    }

    tagValuesByKey.forEach(autocompleteTagsCache::put);
  }
}
