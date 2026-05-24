CREATE TABLE IF NOT EXISTS service_operation_names
(
  service_name   LowCardinality(String),
  operation_name LowCardinality(String)
) ENGINE = ReplacingMergeTree
    ORDER BY (service_name, operation_name);

CREATE TABLE IF NOT EXISTS dependencies
(
  timestamp           DateTime64(6),
  local_service_name  LowCardinality(String),
  remote_service_name LowCardinality(String),
  call_count          UInt64 DEFAULT 1,
  error_count         UInt64 DEFAULT 0
) ENGINE = SummingMergeTree((call_count, error_count))
    ORDER BY (local_service_name, remote_service_name, timestamp)
    PARTITION BY toDate(timestamp);

CREATE TABLE IF NOT EXISTS spans
(
  trace_id                     UInt64 CODEC(T64, ZSTD(1)),
  trace_id_high                UInt64 DEFAULT 0 CODEC(ZSTD(1)),
  parent_id                    UInt64 DEFAULT 0 CODEC(ZSTD(1)),
  span_id                      UInt64 CODEC(T64, ZSTD(1)),
  kind                         LowCardinality(String) CODEC(ZSTD(1)),
  name                         LowCardinality(String) CODEC(ZSTD(1)),
  timestamp                    DATETIME64(6) CODEC(Delta(8), ZSTD(1)),
  duration                     UInt64 CODEC(T64, ZSTD(1)),
  local_endpoint_service_name  LowCardinality(String) CODEC(ZSTD(1)),
  local_endpoint_ipv4          IPv4   DEFAULT toIPv4('0.0.0.0') CODEC(Delta(4), ZSTD(1)),
  local_endpoint_ipv6          IPv6   DEFAULT toIPv6('::') CODEC(ZSTD(1)),
  local_endpoint_port          UInt16 DEFAULT 0 CODEC(ZSTD(1)),
  remote_endpoint_service_name LowCardinality(String) CODEC(ZSTD(1)),
  remote_endpoint_ipv4         IPv4   DEFAULT toIPv4('0.0.0.0') CODEC(Delta(4), ZSTD(1)),
  remote_endpoint_ipv6         IPv6   DEFAULT toIPv6('::') CODEC(ZSTD(1)),
  remote_endpoint_port         UInt16 DEFAULT 0 CODEC(ZSTD(1)),
  annotations                  Array (Tuple(timestamp DateTime64(6), value String)) CODEC(ZSTD(3)),
  tags                         Map(String, String) CODEC(ZSTD(3)),
  status_code                  LowCardinality(String) CODEC(ZSTD(1)),
  shared                       UInt8  DEFAULT 0 CODEC(ZSTD(1)),
  debug                        UInt8  DEFAULT 0 CODEC(ZSTD(1))
) ENGINE = MergeTree()
    PARTITION BY toDate(timestamp)
    ORDER BY (trace_id, trace_id_high, timestamp)
    SETTINGS index_granularity = 1024;

ALTER TABLE spans
  ADD PROJECTION spans_idx(
    SELECT trace_id, trace_id_high, timestamp, local_endpoint_service_name, name, duration
    ORDER BY (timestamp, local_endpoint_service_name, name, duration, trace_id)
    );

ALTER TABLE spans
  ADD INDEX IF NOT EXISTS idx_trace_id trace_id TYPE bloom_filter GRANULARITY 4;

CREATE TABLE IF NOT EXISTS spans_aggregate_stats
(
  span_name        String,
  span_kind        String,
  service_name     String,
  median_duration  AggregateFunction(median, Float64),
  average_duration AggregateFunction(avg, Float64),
  p50              AggregateFunction(quantiles(0.5), Float64),
  p95              AggregateFunction(quantiles(0.95), Float64),
  p99              AggregateFunction(quantiles(0.99), Float64),
  success_count    AggregateFunction(sum, UInt64),
  error_count      AggregateFunction(sum, UInt64),
  total_count      AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree
    ORDER BY (span_name, span_kind, service_name);

CREATE MATERIALIZED VIEW spans_aggregate_mv TO spans_aggregate_stats AS
SELECT name                                                       AS span_name,
       kind                                                       AS span_kind,
       local_endpoint_service_name                                AS service_name,
       medianState(CAST(duration AS Float64))                     AS median_duration,
       avgState(CAST(duration AS Float64))                        AS average_duration,
       quantilesState(0.5)(CAST(duration AS Float64))             AS p50,
       quantilesState(0.95)(CAST(duration AS Float64))            AS p95,
       quantilesState(0.99)(CAST(duration AS Float64))            AS p99,
       sumState(CAST(if(status_code != 'error', 1, 0) AS UInt64)) AS success_count,
       sumState(CAST(if(status_code = 'error', 1, 0) AS UInt64))  AS error_count,
       sumState(CAST(1 AS UInt64))                                AS total_count
FROM spans
GROUP BY name, span_kind, service_name
ORDER BY name, span_kind, service_name;
