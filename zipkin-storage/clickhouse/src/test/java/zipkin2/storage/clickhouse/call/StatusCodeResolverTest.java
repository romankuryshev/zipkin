package zipkin2.storage.clickhouse.call;

import org.junit.jupiter.api.Test;
import zipkin2.Endpoint;
import zipkin2.Span;

import static org.junit.jupiter.api.Assertions.*;

class StatusCodeResolverTest {

  @Test
  void resolveStatusCode_withHttpSuccess() {
    Span span = Span.newBuilder()
      .traceId("4e441824ec2b6a44ffdc9bb9a6453df3")
      .id("ffdc9bb9a6453df3")
      .name("test-span")
      .putTag("http.status_code", "200")
      .build();

    String status = StatusCodeResolver.resolveStatusCode(span);
    assertEquals("success", status);
  }

  @Test
  void resolveStatusCode_withHttpError() {
    Span span = Span.newBuilder()
      .traceId("4e441824ec2b6a44ffdc9bb9a6453df3")
      .id("ffdc9bb9a6453df3")
      .name("test-span")
      .putTag("http.status_code", "500")
      .build();

    String status = StatusCodeResolver.resolveStatusCode(span);
    assertEquals("error", status);
  }

  @Test
  void resolveStatusCode_withHttpRedirect() {
    Span span = Span.newBuilder()
      .traceId("4e441824ec2b6a44ffdc9bb9a6453df3")
      .id("ffdc9bb9a6453df3")
      .name("test-span")
      .putTag("http.status_code", "301")
      .build();

    String status = StatusCodeResolver.resolveStatusCode(span);
    assertEquals("success", status);
  }

  @Test
  void resolveStatusCode_withGrpcSuccess() {
    Span span = Span.newBuilder()
      .traceId("4e441824ec2b6a44ffdc9bb9a6453df3")
      .id("ffdc9bb9a6453df3")
      .name("test-span")
      .putTag("grpc.status", "0")
      .build();

    String status = StatusCodeResolver.resolveStatusCode(span);
    assertEquals("success", status);
  }

  @Test
  void resolveStatusCode_withGrpcError() {
    Span span = Span.newBuilder()
      .traceId("4e441824ec2b6a44ffdc9bb9a6453df3")
      .id("ffdc9bb9a6453df3")
      .name("test-span")
      .putTag("grpc.status", "2")
      .build();

    String status = StatusCodeResolver.resolveStatusCode(span);
    assertEquals("error", status);
  }

  @Test
  void resolveStatusCode_withErrorTag() {
    Span span = Span.newBuilder()
      .traceId("4e441824ec2b6a44ffdc9bb9a6453df3")
      .id("ffdc9bb9a6453df3")
      .name("test-span")
      .putTag("error", "true")
      .build();

    String status = StatusCodeResolver.resolveStatusCode(span);
    assertEquals("error", status);
  }

  @Test
  void resolveStatusCode_withExceptionTag() {
    Span span = Span.newBuilder()
      .traceId("4e441824ec2b6a44ffdc9bb9a6453df3")
      .id("ffdc9bb9a6453df3")
      .name("test-span")
      .putTag("exception", "NullPointerException")
      .build();

    String status = StatusCodeResolver.resolveStatusCode(span);
    assertEquals("error", status);
  }

  @Test
  void resolveStatusCode_withServerSpanError() {
    Span span = Span.newBuilder()
      .traceId("4e441824ec2b6a44ffdc9bb9a6453df3")
      .id("ffdc9bb9a6453df3")
      .name("handle-request")
      .kind(Span.Kind.SERVER)
      .putTag("error", "true")
      .build();

    String status = StatusCodeResolver.resolveStatusCode(span);
    assertEquals("error", status);
  }

  @Test
  void resolveStatusCode_withExplicitStatusCode() {
    Span span = Span.newBuilder()
      .traceId("4e441824ec2b6a44ffdc9bb9a6453df3")
      .id("ffdc9bb9a6453df3")
      .name("test-span")
      .putTag("status.code", "CUSTOM_ERROR")
      .putTag("http.status_code", "200")
      .build();

    String status = StatusCodeResolver.resolveStatusCode(span);
    assertEquals("CUSTOM_ERROR", status);
  }

  @Test
  void resolveStatusCode_emptySpan() {
    Span span = Span.newBuilder()
      .traceId("4e441824ec2b6a44ffdc9bb9a6453df3")
      .id("ffdc9bb9a6453df3")
      .name("test-span")
      .build();

    String status = StatusCodeResolver.resolveStatusCode(span);
    assertEquals("", status);
  }

  @Test
  void resolveStatusCode_nullSpan() {
    String status = StatusCodeResolver.resolveStatusCode(null);
    assertEquals("", status);
  }

  @Test
  void resolveStatusCode_httpClientError() {
    Span span = Span.newBuilder()
      .traceId("4e441824ec2b6a44ffdc9bb9a6453df3")
      .id("ffdc9bb9a6453df3")
      .name("test-span")
      .putTag("http.status_code", "404")
      .build();

    String status = StatusCodeResolver.resolveStatusCode(span);
    assertEquals("error", status);
  }

  @Test
  void resolveStatusCode_httpBadRequest() {
    Span span = Span.newBuilder()
      .traceId("4e441824ec2b6a44ffdc9bb9a6453df3")
      .id("ffdc9bb9a6453df3")
      .name("test-span")
      .putTag("http.status_code", "400")
      .build();

    String status = StatusCodeResolver.resolveStatusCode(span);
    assertEquals("error", status);
  }

  @Test
  void resolveStatusCode_producerSpanNoStatus() {
    // Для PRODUCER/CONSUMER span'ов без явного статуса не присваиваем success
    Span span = Span.newBuilder()
      .traceId("4e441824ec2b6a44ffdc9bb9a6453df3")
      .id("ffdc9bb9a6453df3")
      .name("publish-message")
      .kind(Span.Kind.PRODUCER)
      .build();

    String status = StatusCodeResolver.resolveStatusCode(span);
    assertEquals("", status);
  }

  @Test
  void resolveStatusCode_consumerSpanWithError() {
    Span span = Span.newBuilder()
      .traceId("4e441824ec2b6a44ffdc9bb9a6453df3")
      .id("ffdc9bb9a6453df3")
      .name("consume-message")
      .kind(Span.Kind.CONSUMER)
      .putTag("error", "true")
      .build();

    String status = StatusCodeResolver.resolveStatusCode(span);
    assertEquals("error", status);
  }
}

