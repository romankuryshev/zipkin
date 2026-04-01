package zipkin2.storage.clickhouse.dto;

/**
 * DTO for dependencies table in ClickHouse.
 * Maps to columns: local_service_name, remote_service_name
 */
public class DependencyRecord {
  private String localServiceName;
  private String remoteServiceName;

  public DependencyRecord() {
  }

  public DependencyRecord(String localServiceName, String remoteServiceName) {
    this.localServiceName = localServiceName;
    this.remoteServiceName = remoteServiceName;
  }

  public String getLocalServiceName() {
    return localServiceName;
  }

  public void setLocalServiceName(String localServiceName) {
    this.localServiceName = localServiceName;
  }

  public String getRemoteServiceName() {
    return remoteServiceName;
  }

  public void setRemoteServiceName(String remoteServiceName) {
    this.remoteServiceName = remoteServiceName;
  }

  @Override
  public String toString() {
    return "DependencyRecord{" +
      "localServiceName='" + localServiceName + '\'' +
      ", remoteServiceName='" + remoteServiceName + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DependencyRecord that = (DependencyRecord) o;

    if (localServiceName != null ? !localServiceName.equals(that.localServiceName) : that.localServiceName != null)
      return false;
    return remoteServiceName != null ? remoteServiceName.equals(that.remoteServiceName) : that.remoteServiceName == null;
  }

  @Override
  public int hashCode() {
    int result = localServiceName != null ? localServiceName.hashCode() : 0;
    result = 31 * result + (remoteServiceName != null ? remoteServiceName.hashCode() : 0);
    return result;
  }
}
