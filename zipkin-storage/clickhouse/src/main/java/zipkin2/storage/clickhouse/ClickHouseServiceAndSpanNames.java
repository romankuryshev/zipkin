package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.clickhouse.call.GetRemoteServiceNamesCall;
import zipkin2.storage.clickhouse.call.GetServiceNamesCall;
import zipkin2.storage.clickhouse.call.GetSpanNamesCall;

import java.util.List;

/**
 * ClickHouse implementation of ServiceAndSpanNames interface.
 * Provides autocomplete functionality for service and span names.
 */
public class ClickHouseServiceAndSpanNames implements ServiceAndSpanNames {

  private final Client client;
  private final String database;

  public ClickHouseServiceAndSpanNames(Client client, String database) {
    this.client = client;
    this.database = database;
  }

  /**
   * Retrieves all service names from ClickHouse, sorted lexicographically.
   */
  @Override
  public Call<List<String>> getServiceNames() {
    return new GetServiceNamesCall(client, database);
  }

  /**
   * Retrieves all remote service names recorded by a specific service.
   * Remote service names are extracted from span.remoteEndpoint.serviceName.
   */
  @Override
  public Call<List<String>> getRemoteServiceNames(String serviceName) {
    return new GetRemoteServiceNamesCall(client, database, serviceName);
  }

  /**
   * Retrieves all span names recorded by a specific service, sorted lexicographically.
   */
  @Override
  public Call<List<String>> getSpanNames(String serviceName) {
    return new GetSpanNamesCall(client, database, serviceName);
  }

  @Override
  public String toString() {
    return "ClickHouseServiceAndSpanNames{" +
      "database='" + database + '\'' +
      '}';
  }
}
