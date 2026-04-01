package zipkin2.storage.clickhouse.dto;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * DTO for spans table in ClickHouse.
 * Maps to table structure with all columns.
 */
public class SpanRecord {
  private long traceId;
  private long traceIdHigh;
  private Long parentId;
  private long spanId;
  private String kind;
  private String name;
  private Instant timestamp;
  private long duration;
  private String localEndpointServiceName;
  private Inet4Address localEndpointIpv4;
  private Inet6Address localEndpointIpv6;
  private Integer localEndpointPort;
  private String remoteEndpointServiceName;
  private Inet4Address remoteEndpointIpv4;
  private Inet6Address remoteEndpointIpv6;
  private Integer remoteEndpointPort;
  private List<Object[]> annotations;
  private Map<String, String> tags;
  private String statusCode;

  public SpanRecord() {
  }

  public SpanRecord(long traceId, long traceIdHigh, Long parentId, long spanId,
                    String kind, String name, Instant timestamp, long duration,
                    String localEndpointServiceName, Inet4Address localEndpointIpv4,
                    Inet6Address localEndpointIpv6, Integer localEndpointPort,
                    String remoteEndpointServiceName, Inet4Address remoteEndpointIpv4,
                    Inet6Address remoteEndpointIpv6, Integer remoteEndpointPort,
                    List<Object[]> annotations, Map<String, String> tags,
                    String statusCode) {
    this.traceId = traceId;
    this.traceIdHigh = traceIdHigh;
    this.parentId = parentId;
    this.spanId = spanId;
    this.kind = kind;
    this.name = name;
    this.timestamp = timestamp;
    this.duration = duration;
    this.localEndpointServiceName = localEndpointServiceName;
    this.localEndpointIpv4 = localEndpointIpv4;
    this.localEndpointIpv6 = localEndpointIpv6;
    this.localEndpointPort = localEndpointPort;
    this.remoteEndpointServiceName = remoteEndpointServiceName;
    this.remoteEndpointIpv4 = remoteEndpointIpv4;
    this.remoteEndpointIpv6 = remoteEndpointIpv6;
    this.remoteEndpointPort = remoteEndpointPort;
    this.annotations = annotations;
    this.tags = tags;
    this.statusCode = statusCode;
  }

  // Getters and Setters
  public long getTraceId() { return traceId; }
  public void setTraceId(long traceId) { this.traceId = traceId; }
  public long getTraceIdHigh() { return traceIdHigh; }
  public void setTraceIdHigh(long traceIdHigh) { this.traceIdHigh = traceIdHigh; }
  public Long getParentId() { return parentId; }
  public void setParentId(Long parentId) { this.parentId = parentId; }
  public long getSpanId() { return spanId; }
  public void setSpanId(long spanId) { this.spanId = spanId; }
  public String getKind() { return kind; }
  public void setKind(String kind) { this.kind = kind; }
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  public Instant getTimestamp() { return timestamp; }
  public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
  public long getDuration() { return duration; }
  public void setDuration(long duration) { this.duration = duration; }
  public String getLocalEndpointServiceName() { return localEndpointServiceName; }
  public void setLocalEndpointServiceName(String localEndpointServiceName) { this.localEndpointServiceName = localEndpointServiceName; }
  public Inet4Address getLocalEndpointIpv4() { return localEndpointIpv4; }
  public void setLocalEndpointIpv4(Inet4Address localEndpointIpv4) { this.localEndpointIpv4 = localEndpointIpv4; }
  public Inet6Address getLocalEndpointIpv6() { return localEndpointIpv6; }
  public void setLocalEndpointIpv6(Inet6Address localEndpointIpv6) { this.localEndpointIpv6 = localEndpointIpv6; }
  public Integer getLocalEndpointPort() { return localEndpointPort; }
  public void setLocalEndpointPort(Integer localEndpointPort) { this.localEndpointPort = localEndpointPort; }
  public String getRemoteEndpointServiceName() { return remoteEndpointServiceName; }
  public void setRemoteEndpointServiceName(String remoteEndpointServiceName) { this.remoteEndpointServiceName = remoteEndpointServiceName; }
  public Inet4Address getRemoteEndpointIpv4() { return remoteEndpointIpv4; }
  public void setRemoteEndpointIpv4(Inet4Address remoteEndpointIpv4) { this.remoteEndpointIpv4 = remoteEndpointIpv4; }
  public Inet6Address getRemoteEndpointIpv6() { return remoteEndpointIpv6; }
  public void setRemoteEndpointIpv6(Inet6Address remoteEndpointIpv6) { this.remoteEndpointIpv6 = remoteEndpointIpv6; }
  public Integer getRemoteEndpointPort() { return remoteEndpointPort; }
  public void setRemoteEndpointPort(Integer remoteEndpointPort) { this.remoteEndpointPort = remoteEndpointPort; }
  public List<Object[]> getAnnotations() { return annotations; }
  public void setAnnotations(List<Object[]> annotations) { this.annotations = annotations; }
  public Map<String, String> getTags() { return tags; }
  public void setTags(Map<String, String> tags) { this.tags = tags; }
  public String getStatusCode() { return statusCode; }
  public void setStatusCode(String statusCode) { this.statusCode = statusCode; }

  @Override
  public String toString() {
    return "SpanRecord{" +
      "traceId=" + traceId +
      ", traceIdHigh=" + traceIdHigh +
      ", spanId=" + spanId +
      ", name='" + name + '\'' +
      '}';
  }
}
