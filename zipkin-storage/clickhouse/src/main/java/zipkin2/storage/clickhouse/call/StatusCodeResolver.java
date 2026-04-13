package zipkin2.storage.clickhouse.call;

import zipkin2.Span;
import java.util.Map;

public final class StatusCodeResolver {

  private StatusCodeResolver() {}

  public static String resolveStatusCode(Span span) {
    if (span == null) {
      return "";
    }

    Map<String, String> tags = span.tags();
    if (tags == null || tags.isEmpty()) {
      return "";
    }

    String explicitStatus = tags.get("status.code");
    if (explicitStatus != null && !explicitStatus.isEmpty()) {
      return explicitStatus;
    }

    if (hasErrorTags(tags)) {
      return "error";
    }

    String httpStatus = resolveHttpStatus(tags);
    if (!httpStatus.isEmpty()) {
      return httpStatus;
    }

    String grpcStatus = resolveGrpcStatus(tags);
    if (!grpcStatus.isEmpty()) {
      return grpcStatus;
    }

    String heuristicStatus = resolveBySpanKind(span);
    if (!heuristicStatus.isEmpty()) {
      return heuristicStatus;
    }

    return "";
  }

  private static boolean hasErrorTags(Map<String, String> tags) {
    if ("true".equals(tags.get("error"))) {
      return true;
    }

    return tags.containsKey("exception") ||
           tags.containsKey("exception.message") ||
           tags.containsKey("exception.type");
  }

  private static String resolveHttpStatus(Map<String, String> tags) {
    String httpStatusCode = tags.get("http.status_code");
    if (httpStatusCode == null || httpStatusCode.isEmpty()) {
      return "";
    }

    try {
      int statusCode = Integer.parseInt(httpStatusCode);

      if (statusCode >= 200 && statusCode < 400) {
        return "success";
      }

      if (statusCode >= 400) {
        return "error";
      }

      return httpStatusCode;
    } catch (NumberFormatException e) {
      return httpStatusCode;
    }
  }

  private static String resolveGrpcStatus(Map<String, String> tags) {
    String grpcStatus = tags.get("grpc.status");
    if (grpcStatus == null || grpcStatus.isEmpty()) {
      return "";
    }

    try {
      int status = Integer.parseInt(grpcStatus);

      if (status == 0) {
        return "success";
      }

      return "error";
    } catch (NumberFormatException e) {
      return grpcStatus;
    }
  }

  private static String resolveBySpanKind(Span span) {
    Span.Kind kind = span.kind();

    if (kind == Span.Kind.CLIENT || kind == Span.Kind.SERVER) {
      Map<String, String> tags = span.tags();
      if (tags != null && !tags.containsKey("error") && !tags.containsKey("exception")) {
        return "success";
      }
    }

    return "";
  }
}

