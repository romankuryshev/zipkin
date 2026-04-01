package zipkin2.storage.clickhouse.dto;

import java.time.Instant;

/**
 * DTO for annotation tuple in ClickHouse.
 * Maps to Array(Tuple(timestamp DateTime64(6), value String))
 */
public class AnnotationRecord {
  private Instant timestamp;
  private String value;

  public AnnotationRecord() {
  }

  public AnnotationRecord(Instant timestamp, String value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Instant timestamp) {
    this.timestamp = timestamp;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "AnnotationRecord{" +
      "timestamp=" + timestamp +
      ", value='" + value + '\'' +
      '}';
  }
}

