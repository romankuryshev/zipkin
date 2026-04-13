package zipkin2.storage.clickhouse;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ClickHouseClusterTest {

  @Test
  public void testSingleHostBackwardCompatibility() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .setHost("localhost")
      .setPort(8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    assertNotNull(builder);
  }

  @Test
  public void testAddSingleClusterNode() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .addClusterNode("localhost", 8123)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    assertNotNull(builder);
  }

  @Test
  public void testAddMultipleClusterNodes() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .addClusterNode("node1", 8123)
      .addClusterNode("node2", 8124)
      .addClusterNode("node3", 8125)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    assertNotNull(builder);
  }

  @Test
  public void testSetClusterNodes() {
    List<String> nodes = Arrays.asList(
      "http://node1:8123/",
      "http://node2:8124/",
      "http://node3:8125/"
    );

    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder()
      .setClusterNodes(nodes)
      .setDatabase("zipkin")
      .setUsername("zipkin")
      .setPassword("zipkin");

    assertNotNull(builder);
  }

  @Test
  public void testAddClusterNodeInvalidHost() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder();
    assertThrows(NullPointerException.class, () -> builder.addClusterNode(null, 8123));
  }

  @Test
  public void testAddClusterNodeInvalidPort() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder();
    assertThrows(IllegalArgumentException.class, () -> builder.addClusterNode("localhost", -1));
  }

  @Test
  public void testSetClusterNodesNull() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder();
    assertThrows(NullPointerException.class, () -> builder.setClusterNodes(null));
  }

  @Test
  public void testSetClusterNodesEmpty() {
    ClickHouseStorage.Builder builder = new ClickHouseStorage.Builder();
    assertThrows(IllegalArgumentException.class, () -> builder.setClusterNodes(Arrays.asList()));
  }
}

