package zipkin2.storage;

/**
 * Statistics about spans with a given service and span name.
 */
public final class SpanStatistics {
  public final String spanName;
  public final String spanKind;
  public final long medianDuration;
  public final long averageDuration;
  public final long p50;
  public final long p95;
  public final long p99;
  public final long successCount;
  public final long errorCount;
  public final long totalCount;

  public SpanStatistics(String spanName, String spanKind, long medianDuration, long averageDuration,
      long p50, long p95, long p99, long successCount, long errorCount, long totalCount) {
    this.spanName = spanName;
    this.spanKind = spanKind;
    this.medianDuration = medianDuration;
    this.averageDuration = averageDuration;
    this.p50 = p50;
    this.p95 = p95;
    this.p99 = p99;
    this.successCount = successCount;
    this.errorCount = errorCount;
    this.totalCount = totalCount;
  }

  @Override
  public String toString() {
    return "SpanStatistics{" +
        "spanName='" + spanName + '\'' +
        ", spanKind='" + spanKind + '\'' +
        ", medianDuration=" + medianDuration +
        ", averageDuration=" + averageDuration +
        ", p50=" + p50 +
        ", p95=" + p95 +
        ", p99=" + p99 +
        ", successCount=" + successCount +
        ", errorCount=" + errorCount +
        ", totalCount=" + totalCount +
        '}';
  }
}
