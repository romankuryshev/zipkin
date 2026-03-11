package zipkin2.storage.clickhouse;

import com.clickhouse.client.api.Client;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.clickhouse.call.InsertSpansCall;

import java.util.List;

/**
 * ClickHouse implementation of SpanConsumer using ClickHouse Client v2 (0.9.6+).
 *
 * Inserts spans into the main 'spans' table with the following columns:
 * - trace_id: String
 * - span_id: String
 * - parent_id: String
 * - service_name: LowCardinality(String)
 * - operation_name: LowCardinality(String)
 * - remote_service_name: LowCardinality(String)
 * - span_kind: LowCardinality(String)
 * - start_time: DateTime64(6) - microseconds precision
 * - duration_us: UInt64 - duration in microseconds
 * - tags: Map(String, String)
 * - status_code: LowCardinality(String)
 */
public class ClickHouseSpanConsumer implements SpanConsumer {
  private final Client client;
  private final String database;

  public ClickHouseSpanConsumer(Client client, String database) {
    this.client = client;
    this.database = database;
  }

  /**
   * Accepts a batch of spans and returns a Call that writes them to ClickHouse.
   *
   * @param spans list of spans to store
   * @return Call representing the async write operation
   */
  @Override
  public Call<Void> accept(List<Span> spans) {
    if (spans.isEmpty()) {
      return Call.create(null);
    }

    return new InsertSpansCall(client, database, spans);
  }
}
