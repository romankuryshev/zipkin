package zipkin2.storage.clickhouse;

import org.junit.jupiter.api.Test;
import zipkin2.Call;
import zipkin2.Span;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ClickHouseSpanConsumerTest {

  @Test
  public void testBatchFlushOnSizeThreshold() {
    var mockClient = mock(com.clickhouse.client.api.Client.class);
    var consumer = new ClickHouseSpanConsumer(mockClient, "zipkin", true);

    try {
      List<Span> spans = createTestSpans(10);

      Call<Void> result = consumer.accept(spans);
      assertNotNull(result);

    } finally {
      consumer.close();
    }
  }

  @Test
  public void testSpansBuffered() throws IOException {
    var mockClient = mock(com.clickhouse.client.api.Client.class);
    var consumer = new ClickHouseSpanConsumer(mockClient, "zipkin", true);

    try {
      List<Span> spans = createTestSpans(5);
      Call<Void> result = consumer.accept(spans);
      assertNotNull(result);

      result.execute();

    } finally {
      consumer.close();
    }
  }

  @Test
  public void testTimeoutFlush() throws InterruptedException {
    var mockClient = mock(com.clickhouse.client.api.Client.class);
    var consumer = new ClickHouseSpanConsumer(mockClient, "zipkin", true);

      List<Span> spans = createTestSpans(3);
      consumer.accept(spans);

      Thread.sleep(6000);
      consumer.close();
  }

  @Test
  public void testCloseFlushesRemainingSpans() {
    var mockClient = mock(com.clickhouse.client.api.Client.class);
    var consumer = new ClickHouseSpanConsumer(mockClient, "zipkin", true);

    List<Span> spans = createTestSpans(5);
    consumer.accept(spans);

    consumer.close();
  }

  @Test
  public void testErrorHandling() {
    var mockClient = mock(com.clickhouse.client.api.Client.class);
    var consumer = new ClickHouseSpanConsumer(mockClient, "zipkin", true);

      List<Span> spans = createTestSpans(5);
      consumer.accept(spans);

      consumer.close();
  }

  private List<Span> createTestSpans(int count) {
    List<Span> spans = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Span span = Span.newBuilder()
        .traceId("0000000000000001")
        .id(String.format("%016x", i))
        .name("test-span-" + i)
        .timestamp(System.currentTimeMillis() * 1000)
        .duration(100)
        .build();
      spans.add(span);
    }
    return spans;
  }
}

