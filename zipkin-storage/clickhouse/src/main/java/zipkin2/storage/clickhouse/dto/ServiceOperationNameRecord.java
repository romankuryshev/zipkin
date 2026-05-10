package zipkin2.storage.clickhouse.dto;

public class ServiceOperationNameRecord {
  private String serviceName;
  private String operationName;

  public ServiceOperationNameRecord() {
  }

  public ServiceOperationNameRecord(String serviceName, String operationName) {
    this.serviceName = serviceName;
    this.operationName = operationName;
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public String getOperationName() {
    return operationName;
  }

  public void setOperationName(String operationName) {
    this.operationName = operationName;
  }

  @Override
  public String toString() {
    return "ServiceOperationNameRecord{" +
      "serviceName='" + serviceName + '\'' +
      ", operationName='" + operationName + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ServiceOperationNameRecord that = (ServiceOperationNameRecord) o;

    if (serviceName != null ? !serviceName.equals(that.serviceName) : that.serviceName != null)
      return false;
    return operationName != null ? operationName.equals(that.operationName) : that.operationName == null;
  }

  @Override
  public int hashCode() {
    int result = serviceName != null ? serviceName.hashCode() : 0;
    result = 31 * result + (operationName != null ? operationName.hashCode() : 0);
    return result;
  }
}
