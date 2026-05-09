package zipkin2.storage.clickhouse.dto;

import java.time.Instant;

public class DependencyRecord {
  private Instant timestamp;
  private String localServiceName;
  private String remoteServiceName;
  private long callCount;
  private long errorCount;

  public DependencyRecord() {
  }

  public DependencyRecord(Instant timestamp, String localServiceName, String remoteServiceName,
                          long callCount, long errorCount) {
    this.timestamp = timestamp;
    this.localServiceName = localServiceName;
    this.remoteServiceName = remoteServiceName;
    this.callCount = callCount;
    this.errorCount = errorCount;
  }

  public Instant getTimestamp() { return timestamp; }
  public void setTimestamp(Instant v) { this.timestamp = v; }

  public String getLocalServiceName() { return localServiceName; }
  public void setLocalServiceName(String v) { this.localServiceName = v; }

  public String getRemoteServiceName() { return remoteServiceName; }
  public void setRemoteServiceName(String v) { this.remoteServiceName = v; }

  public long getCallCount() { return callCount; }
  public void setCallCount(long v) { this.callCount = v; }

  public long getErrorCount() { return errorCount; }
  public void setErrorCount(long v) { this.errorCount = v; }

  @Override
  public String toString() {
    return "DependencyRecord{" + localServiceName + "->" + remoteServiceName
      + " calls=" + callCount + " errors=" + errorCount + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DependencyRecord)) return false;
    DependencyRecord that = (DependencyRecord) o;
    return localServiceName != null ? localServiceName.equals(that.localServiceName)
        : that.localServiceName == null
      && (remoteServiceName != null ? remoteServiceName.equals(that.remoteServiceName)
          : that.remoteServiceName == null);
  }

  @Override
  public int hashCode() {
    int result = localServiceName != null ? localServiceName.hashCode() : 0;
    result = 31 * result + (remoteServiceName != null ? remoteServiceName.hashCode() : 0);
    return result;
  }
}
