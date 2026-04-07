package zipkin2.storage;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Statistics about spans with a given service and span name.
 */
public final class SpanStatistics {
  public final String spanName;
  public final String spanKind;
  public final BigDecimal medianDuration;
  public final BigDecimal averageDuration;
  public final BigDecimal p50;
  public final BigDecimal p95;
  public final BigDecimal p99;
  public final long successCount;
  public final long errorCount;
  public final long totalCount;

  public SpanStatistics(String spanName, String spanKind, BigDecimal medianDuration,
      BigDecimal averageDuration, BigDecimal p50, BigDecimal p95, BigDecimal p99,
      long successCount, long errorCount, long totalCount) {
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
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SpanStatistics that = (SpanStatistics) o;
    return successCount == that.successCount &&
        errorCount == that.errorCount &&
        totalCount == that.totalCount &&
        Objects.equals(spanName, that.spanName) &&
        Objects.equals(spanKind, that.spanKind) &&
        Objects.equals(medianDuration, that.medianDuration) &&
        Objects.equals(averageDuration, that.averageDuration) &&
        Objects.equals(p50, that.p50) &&
        Objects.equals(p95, that.p95) &&
        Objects.equals(p99, that.p99);
  }

  @Override
  public int hashCode() {
    return Objects.hash(spanName, spanKind, medianDuration, averageDuration, p50, p95, p99,
        successCount, errorCount, totalCount);
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
