/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.internal;

import java.util.Iterator;
import java.util.Map;
import zipkin2.Annotation;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.SpanStatistics;

import static zipkin2.internal.JsonEscaper.jsonEscape;
import static zipkin2.internal.JsonEscaper.jsonEscapedSizeInBytes;
import static zipkin2.internal.WriteBuffer.asciiSizeInBytes;

// @Immutable
public final class V2SpanWriter implements WriteBuffer.Writer<Span> {
  @Override public int sizeInBytes(Span value) {
    int sizeInBytes = 13; // {"traceId":""
    sizeInBytes += value.traceId().length();
    if (value.parentId() != null) {
      sizeInBytes += 30; // ,"parentId":"0123456789abcdef"
    }
    sizeInBytes += 24; // ,"id":"0123456789abcdef"
    if (value.kind() != null) {
      sizeInBytes += 10; // ,"kind":""
      sizeInBytes += value.kind().name().length();
    }
    if (value.name() != null) {
      sizeInBytes += 10; // ,"name":""
      sizeInBytes += jsonEscapedSizeInBytes(value.name());
    }
    if (value.timestampAsLong() != 0L) {
      sizeInBytes += 13; // ,"timestamp":
      sizeInBytes += asciiSizeInBytes(value.timestampAsLong());
    }
    if (value.durationAsLong() != 0L) {
      sizeInBytes += 12; // ,"duration":
      sizeInBytes += asciiSizeInBytes(value.durationAsLong());
    }
    if (value.localEndpoint() != null) {
      sizeInBytes += 17; // ,"localEndpoint":
      sizeInBytes += endpointSizeInBytes(value.localEndpoint(), false);
    }
    if (value.remoteEndpoint() != null) {
      sizeInBytes += 18; // ,"remoteEndpoint":
      sizeInBytes += endpointSizeInBytes(value.remoteEndpoint(), false);
    }
    if (!value.annotations().isEmpty()) {
      sizeInBytes += 17; // ,"annotations":[]
      int length = value.annotations().size();
      if (length > 1) sizeInBytes += length - 1; // comma to join elements
      for (int i = 0; i < length; i++) {
        Annotation a = value.annotations().get(i);
        sizeInBytes += annotationSizeInBytes(a.timestamp(), a.value(), 0);
      }
    }
    if (!value.tags().isEmpty()) {
      sizeInBytes += 10; // ,"tags":{}
      int tagCount = value.tags().size();
      if (tagCount > 1) sizeInBytes += tagCount - 1; // comma to join elements
      for (Map.Entry<String, String> entry : value.tags().entrySet()) {
        sizeInBytes += 5; // "":""
        sizeInBytes += jsonEscapedSizeInBytes(entry.getKey());
        sizeInBytes += jsonEscapedSizeInBytes(entry.getValue());
      }
    }
    if (Boolean.TRUE.equals(value.debug())) {
      sizeInBytes += 13; // ,"debug":true
    }
    if (Boolean.TRUE.equals(value.shared())) {
      sizeInBytes += 14; // ,"shared":true
    }
    if (value.statistics() != null) {
      sizeInBytes += statisticsSizeInBytes(value.statistics());
    }
    return ++sizeInBytes; // }
  }

  @Override public void write(Span value, WriteBuffer b) {
    b.writeAscii("{\"traceId\":\"");
    b.writeAscii(value.traceId());
    b.writeByte('"');
    if (value.parentId() != null) {
      b.writeAscii(",\"parentId\":\"");
      b.writeAscii(value.parentId());
      b.writeByte('"');
    }
    b.writeAscii(",\"id\":\"");
    b.writeAscii(value.id());
    b.writeByte('"');
    if (value.kind() != null) {
      b.writeAscii(",\"kind\":\"");
      b.writeAscii(value.kind().toString());
      b.writeByte('"');
    }
    if (value.name() != null) {
      b.writeAscii(",\"name\":\"");
      b.writeUtf8(jsonEscape(value.name()));
      b.writeByte('"');
    }
    if (value.timestampAsLong() != 0L) {
      b.writeAscii(",\"timestamp\":");
      b.writeAscii(value.timestampAsLong());
    }
    if (value.durationAsLong() != 0L) {
      b.writeAscii(",\"duration\":");
      b.writeAscii(value.durationAsLong());
    }
    if (value.localEndpoint() != null) {
      b.writeAscii(",\"localEndpoint\":");
      writeEndpoint(value.localEndpoint(), b, false);
    }
    if (value.remoteEndpoint() != null) {
      b.writeAscii(",\"remoteEndpoint\":");
      writeEndpoint(value.remoteEndpoint(), b, false);
    }
    if (!value.annotations().isEmpty()) {
      b.writeAscii(",\"annotations\":");
      b.writeByte('[');
      for (int i = 0, length = value.annotations().size(); i < length; ) {
        Annotation a = value.annotations().get(i++);
        writeAnnotation(a.timestamp(), a.value(), null, b);
        if (i < length) b.writeByte(',');
      }
      b.writeByte(']');
    }
    if (!value.tags().isEmpty()) {
      b.writeAscii(",\"tags\":{");
      Iterator<Map.Entry<String, String>> i = value.tags().entrySet().iterator();
      while (i.hasNext()) {
        Map.Entry<String, String> entry = i.next();
        b.writeByte('"');
        b.writeUtf8(jsonEscape(entry.getKey()));
        b.writeAscii("\":\"");
        b.writeUtf8(jsonEscape(entry.getValue()));
        b.writeByte('"');
        if (i.hasNext()) b.writeByte(',');
      }
      b.writeByte('}');
    }
    if (Boolean.TRUE.equals(value.debug())) {
      b.writeAscii(",\"debug\":true");
    }
    if (Boolean.TRUE.equals(value.shared())) {
      b.writeAscii(",\"shared\":true");
    }
    if (value.statistics() != null) {
      writeStatistics(value.statistics(), b);
    }
    b.writeByte('}');
  }

  @Override public String toString() {
    return "Span";
  }

  static int endpointSizeInBytes(Endpoint value, boolean writeEmptyServiceName) {
    int sizeInBytes = 1; // {
    String serviceName = value.serviceName();
    if (serviceName == null && writeEmptyServiceName) serviceName = "";
    if (serviceName != null) {
      sizeInBytes += 16; // "serviceName":""
      sizeInBytes += jsonEscapedSizeInBytes(serviceName);
    }
    if (value.ipv4() != null) {
      if (sizeInBytes != 1) sizeInBytes++; // ,
      sizeInBytes += 9; // "ipv4":""
      sizeInBytes += value.ipv4().length();
    }
    if (value.ipv6() != null) {
      if (sizeInBytes != 1) sizeInBytes++; // ,
      sizeInBytes += 9; // "ipv6":""
      sizeInBytes += value.ipv6().length();
    }
    int port = value.portAsInt();
    if (port != 0) {
      if (sizeInBytes != 1) sizeInBytes++; // ,
      sizeInBytes += 7; // "port":
      sizeInBytes += asciiSizeInBytes(port);
    }
    return ++sizeInBytes; // }
  }

  static void writeEndpoint(Endpoint value, WriteBuffer b, boolean writeEmptyServiceName) {
    b.writeByte('{');
    boolean wroteField = false;
    String serviceName = value.serviceName();
    if (serviceName == null && writeEmptyServiceName) serviceName = "";
    if (serviceName != null) {
      b.writeAscii("\"serviceName\":\"");
      b.writeUtf8(jsonEscape(serviceName));
      b.writeByte('"');
      wroteField = true;
    }
    if (value.ipv4() != null) {
      if (wroteField) b.writeByte(',');
      b.writeAscii("\"ipv4\":\"");
      b.writeAscii(value.ipv4());
      b.writeByte('"');
      wroteField = true;
    }
    if (value.ipv6() != null) {
      if (wroteField) b.writeByte(',');
      b.writeAscii("\"ipv6\":\"");
      b.writeAscii(value.ipv6());
      b.writeByte('"');
      wroteField = true;
    }
    int port = value.portAsInt();
    if (port != 0) {
      if (wroteField) b.writeByte(',');
      b.writeAscii("\"port\":");
      b.writeAscii(port);
    }
    b.writeByte('}');
  }

  static int annotationSizeInBytes(long timestamp, String value, int endpointSizeInBytes) {
    int sizeInBytes = 25; // {"timestamp":,"value":""}
    sizeInBytes += asciiSizeInBytes(timestamp);
    sizeInBytes += jsonEscapedSizeInBytes(value);
    if (endpointSizeInBytes != 0) {
      sizeInBytes += 12; // ,"endpoint":
      sizeInBytes += endpointSizeInBytes;
    }
    return sizeInBytes;
  }

  static void writeAnnotation(long timestamp, String value, @Nullable byte[] endpoint,
    WriteBuffer b) {
    b.writeAscii("{\"timestamp\":");
    b.writeAscii(timestamp);
    b.writeAscii(",\"value\":\"");
    b.writeUtf8(jsonEscape(value));
    b.writeByte('"');
    if (endpoint != null) {
      b.writeAscii(",\"endpoint\":");
      b.write(endpoint);
    }
    b.writeByte('}');
  }

  static int statisticsSizeInBytes(SpanStatistics stats) {
    int sizeInBytes = 15; // ,"statistics":{
    sizeInBytes += 12; // "spanName":"
    sizeInBytes += jsonEscapedSizeInBytes(stats.spanName);
    sizeInBytes += 14; // ","spanKind":"
    sizeInBytes += jsonEscapedSizeInBytes(stats.spanKind);
    sizeInBytes += 19; // ","medianDuration":
    sizeInBytes += stats.medianDuration.toPlainString().length();
    sizeInBytes += 19; // ,"averageDuration":
    sizeInBytes += stats.averageDuration.toPlainString().length();
    sizeInBytes += 7; // ,"p50":
    sizeInBytes += stats.p50.toPlainString().length();
    sizeInBytes += 7; // ,"p95":
    sizeInBytes += stats.p95.toPlainString().length();
    sizeInBytes += 7; // ,"p99":
    sizeInBytes += stats.p99.toPlainString().length();
    sizeInBytes += 16; // ,"successCount":
    sizeInBytes += asciiSizeInBytes(stats.successCount);
    sizeInBytes += 14; // ,"errorCount":
    sizeInBytes += asciiSizeInBytes(stats.errorCount);
    sizeInBytes += 14; // ,"totalCount":
    sizeInBytes += asciiSizeInBytes(stats.totalCount);
    sizeInBytes += 1; // }
    return sizeInBytes;
  }

  static void writeStatistics(SpanStatistics stats, WriteBuffer b) {
    b.writeAscii(",\"statistics\":{");
    b.writeAscii("\"spanName\":\"");
    b.writeUtf8(jsonEscape(stats.spanName));
    b.writeAscii("\",\"spanKind\":\"");
    b.writeUtf8(jsonEscape(stats.spanKind));
    b.writeAscii("\",\"medianDuration\":");
    b.writeAscii(stats.medianDuration.toPlainString());
    b.writeAscii(",\"averageDuration\":");
    b.writeAscii(stats.averageDuration.toPlainString());
    b.writeAscii(",\"p50\":");
    b.writeAscii(stats.p50.toPlainString());
    b.writeAscii(",\"p95\":");
    b.writeAscii(stats.p95.toPlainString());
    b.writeAscii(",\"p99\":");
    b.writeAscii(stats.p99.toPlainString());
    b.writeAscii(",\"successCount\":");
    b.writeAscii(stats.successCount);
    b.writeAscii(",\"errorCount\":");
    b.writeAscii(stats.errorCount);
    b.writeAscii(",\"totalCount\":");
    b.writeAscii(stats.totalCount);
    b.writeByte('}');
  }
}
