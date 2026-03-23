package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.Traces;
import zipkin2.storage.clickhouse.call.GetTraceCall;
import zipkin2.storage.clickhouse.call.GetTracesByIdCall;

import java.util.List;

/**
 * ClickHouse implementation of Traces interface.
 * Allows readback of traces by ID, as written by a SpanConsumer.
 */
public class ClickHouseTraces implements Traces {

  private final Client client;
  private final String database;

  public ClickHouseTraces(Client client, String database) {
    this.client = client;
    this.database = database;
  }

  /**
   * Retrieves spans that share a 128-bit trace id.
   * When strict trace ID is disabled, spans with the same right-most 16 characters are returned.
   *
   * @param traceId the trace ID
   * @return Call that returns a list of spans or empty if none are found
   */
  @Override
  public Call<List<Span>> getTrace(String traceId) {
    return new GetTraceCall(client, database, traceId);
  }

  /**
   * Retrieves any traces with the specified IDs.
   * Results return in any order, and can be empty.
   *
   * @param traceIds a list of unique trace IDs
   * @return Call that returns traces matching the supplied trace IDs, in any order
   */
  @Override
  public Call<List<List<Span>>> getTraces(Iterable<String> traceIds) {
    return new GetTracesByIdCall(client, database, traceIds);
  }

  @Override
  public String toString() {
    return "ClickHouseTraces{" +
      "database='" + database + '\'' +
      '}';
  }
}
