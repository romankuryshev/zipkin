package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import org.junit.jupiter.api.Test;
import zipkin2.Call;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ClickHouseServiceAndSpanNamesTest {

  @Test
  public void getServiceNamesReturnsCallInstance() {
    var mockClient = mock(Client.class);
    var serviceAndSpanNames = new ClickHouseServiceAndSpanNames(mockClient, "zipkin");

    Call<List<String>> result = serviceAndSpanNames.getServiceNames();

    assertNotNull(result);
  }

  @Test
  public void getRemoteServiceNamesReturnsCallInstance() {
    var mockClient = mock(Client.class);
    var serviceAndSpanNames = new ClickHouseServiceAndSpanNames(mockClient, "zipkin");

    Call<List<String>> result = serviceAndSpanNames.getRemoteServiceNames("order-db");

    assertNotNull(result);
  }

  @Test
  public void getRemoteServiceNamesWithDifferentServices() {
    var mockClient = mock(Client.class);
    var serviceAndSpanNames = new ClickHouseServiceAndSpanNames(mockClient, "zipkin");

    Call<List<String>> result1 = serviceAndSpanNames.getRemoteServiceNames("service-1");
    Call<List<String>> result2 = serviceAndSpanNames.getRemoteServiceNames("service-2");

    assertNotNull(result1);
    assertNotNull(result2);
    assertNotEquals(result1, result2);
  }

  @Test
  public void getSpanNamesReturnsCallInstance() {
    var mockClient = mock(Client.class);
    var serviceAndSpanNames = new ClickHouseServiceAndSpanNames(mockClient, "zipkin");

    Call<List<String>> result = serviceAndSpanNames.getSpanNames("order-db");

    assertNotNull(result);
  }

  @Test
  public void getSpanNamesWithDifferentServices() {
    var mockClient = mock(Client.class);
    var serviceAndSpanNames = new ClickHouseServiceAndSpanNames(mockClient, "zipkin");

    Call<List<String>> result1 = serviceAndSpanNames.getSpanNames("order-db");
    Call<List<String>> result2 = serviceAndSpanNames.getSpanNames("payment-service");

    assertNotNull(result1);
    assertNotNull(result2);
    assertNotEquals(result1, result2);
  }

  @Test
  public void getRemoteServiceNamesWithNullServiceName() {
    var mockClient = mock(Client.class);
    var serviceAndSpanNames = new ClickHouseServiceAndSpanNames(mockClient, "zipkin");

    Call<List<String>> result = serviceAndSpanNames.getRemoteServiceNames(null);

    assertNotNull(result);
  }

  @Test
  public void getSpanNamesWithNullServiceName() {
    var mockClient = mock(Client.class);
    var serviceAndSpanNames = new ClickHouseServiceAndSpanNames(mockClient, "zipkin");

    Call<List<String>> result = serviceAndSpanNames.getSpanNames(null);

    assertNotNull(result);
  }

  @Test
  public void constructorWithDifferentDatabase() {
    var mockClient = mock(Client.class);
    var serviceAndSpanNames = new ClickHouseServiceAndSpanNames(mockClient, "custom_db");

    assertNotNull(serviceAndSpanNames);
  }

  @Test
  public void toStringContainsDatabaseName() {
    var mockClient = mock(Client.class);
    var serviceAndSpanNames = new ClickHouseServiceAndSpanNames(mockClient, "zipkin");

    String result = serviceAndSpanNames.toString();

    assertTrue(result.contains("zipkin"));
    assertTrue(result.contains("ClickHouseServiceAndSpanNames"));
  }

  @Test
  public void toStringWithDifferentDatabase() {
    var mockClient = mock(Client.class);
    var serviceAndSpanNames = new ClickHouseServiceAndSpanNames(mockClient, "custom_db");

    String result = serviceAndSpanNames.toString();

    assertTrue(result.contains("custom_db"));
  }

  @Test
  public void multipleCallsToGetServiceNames() {
    var mockClient = mock(Client.class);
    var serviceAndSpanNames = new ClickHouseServiceAndSpanNames(mockClient, "zipkin");

    Call<List<String>> result1 = serviceAndSpanNames.getServiceNames();
    Call<List<String>> result2 = serviceAndSpanNames.getServiceNames();

    assertNotNull(result1);
    assertNotNull(result2);
    assertNotEquals(result1, result2);
  }

  @Test
  public void multipleCallsToGetSpanNamesWithSameService() {
    var mockClient = mock(Client.class);
    var serviceAndSpanNames = new ClickHouseServiceAndSpanNames(mockClient, "zipkin");

    Call<List<String>> result1 = serviceAndSpanNames.getSpanNames("order-db");
    Call<List<String>> result2 = serviceAndSpanNames.getSpanNames("order-db");

    assertNotNull(result1);
    assertNotNull(result2);
    assertNotEquals(result1, result2);
  }

  @Test
  public void getRemoteServiceNamesWithEmptyServiceName() {
    var mockClient = mock(Client.class);
    var serviceAndSpanNames = new ClickHouseServiceAndSpanNames(mockClient, "zipkin");

    Call<List<String>> result = serviceAndSpanNames.getRemoteServiceNames("");

    assertNotNull(result);
  }

  @Test
  public void getSpanNamesWithEmptyServiceName() {
    var mockClient = mock(Client.class);
    var serviceAndSpanNames = new ClickHouseServiceAndSpanNames(mockClient, "zipkin");

    Call<List<String>> result = serviceAndSpanNames.getSpanNames("");

    assertNotNull(result);
  }
}

