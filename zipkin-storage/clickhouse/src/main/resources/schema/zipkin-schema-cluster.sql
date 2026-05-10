CREATE TABLE IF NOT EXISTS service_operation_names_local ON CLUSTER '{cluster}'
(
  service_name   LowCardinality(String),
  operation_name LowCardinality(String)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/service_operation_names', '{replica}')
    ORDER BY (service_name, operation_name);

CREATE TABLE IF NOT EXISTS dependencies_local ON CLUSTER '{cluster}'
(
  timestamp           DateTime64(6),
  local_service_name  LowCardinality(String),
  remote_service_name LowCardinality(String),
  call_count          UInt64 DEFAULT 1,
  error_count         UInt64 DEFAULT 0
) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/dependencies', '{replica}', (call_count, error_count))
    ORDER BY (local_service_name, remote_service_name, timestamp)
    PARTITION BY toDate(timestamp);

CREATE TABLE IF NOT EXISTS spans_local ON CLUSTER '{cluster}'
(
  trace_id                     UInt64,
  trace_id_high                UInt64 DEFAULT 0,
  parent_id                    Nullable(UInt64),
  span_id                      UInt64,
  kind                         LowCardinality(String),
  name                         LowCardinality(String),
  timestamp                    DATETIME64(6),
  duration                     UInt64,
  local_endpoint_service_name  LowCardinality(String),
  local_endpoint_ipv4          Nullable(IPv4),
  local_endpoint_ipv6          Nullable(IPv6),
  local_endpoint_port          Nullable(UInt16),
  remote_endpoint_service_name LowCardinality(String),
  remote_endpoint_ipv4         Nullable(IPv4),
  remote_endpoint_ipv6         Nullable(IPv6),
  remote_endpoint_port         Nullable(UInt16),
  annotations                  Array(Tuple(timestamp DateTime64(6), value String)),
  tags                         Map(String, String),
  status_code                  LowCardinality(String),
  shared                       UInt8 DEFAULT 0,
  debug                        UInt8 DEFAULT 0
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/spans', '{replica}')
    PARTITION BY toDate(timestamp)
    ORDER BY (local_endpoint_service_name, name, timestamp, trace_id)
    SETTINGS index_granularity = 8192;

ALTER TABLE spans_local ON CLUSTER '{cluster}'
  ADD PROJECTION IF NOT EXISTS spans_prj_service_name
    (
    SELECT *
    ORDER BY (trace_id)
    );

ALTER TABLE spans_local ON CLUSTER '{cluster}'
  ADD PROJECTION IF NOT EXISTS spans_prj_service_ts
    (
    SELECT
      local_endpoint_service_name,
      remote_endpoint_service_name,
      name,
      duration,
      timestamp,
      trace_id,
      trace_id_high
    ORDER BY (local_endpoint_service_name, timestamp, trace_id, trace_id_high)
    );

ALTER TABLE spans_local ON CLUSTER '{cluster}' ADD INDEX IF NOT EXISTS idx_ts timestamp TYPE minmax GRANULARITY 4;
ALTER TABLE spans_local ON CLUSTER '{cluster}' ADD INDEX IF NOT EXISTS idx_trace_id trace_id TYPE bloom_filter GRANULARITY 1;

CREATE TABLE IF NOT EXISTS spans_aggregate_stats_local ON CLUSTER '{cluster}'
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
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/spans_aggregate_stats', '{replica}')
    ORDER BY (span_name, span_kind, service_name);

CREATE TABLE IF NOT EXISTS service_operation_names ON CLUSTER '{cluster}'
  AS service_operation_names_local
  ENGINE = Distributed('{cluster}', currentDatabase(), 'service_operation_names_local', rand());

CREATE TABLE IF NOT EXISTS dependencies ON CLUSTER '{cluster}'
  AS dependencies_local
  ENGINE = Distributed('{cluster}', currentDatabase(), 'dependencies_local', rand());

CREATE TABLE IF NOT EXISTS spans ON CLUSTER '{cluster}'
  AS spans_local
  ENGINE = Distributed('{cluster}', currentDatabase(), 'spans_local', rand());

CREATE MATERIALIZED VIEW IF NOT EXISTS spans_aggregate_mv ON CLUSTER '{cluster}'
  TO spans_aggregate_stats_local AS
SELECT name                                                        AS span_name,
       kind                                                        AS span_kind,
       local_endpoint_service_name                                 AS service_name,
       medianState(CAST(duration AS Float64))                      AS median_duration,
       avgState(CAST(duration AS Float64))                         AS average_duration,
       quantilesState(0.5)(CAST(duration AS Float64))              AS p50,
       quantilesState(0.95)(CAST(duration AS Float64))             AS p95,
       quantilesState(0.99)(CAST(duration AS Float64))             AS p99,
       sumState(CAST(if(status_code != 'error', 1, 0) AS UInt64))  AS success_count,
       sumState(CAST(if(status_code = 'error', 1, 0) AS UInt64))   AS error_count,
       sumState(CAST(1 AS UInt64))                                 AS total_count
FROM spans_local
GROUP BY name, span_kind, service_name
ORDER BY name, span_kind, service_name;

CREATE TABLE IF NOT EXISTS spans_aggregate_stats ON CLUSTER '{cluster}'
  AS spans_aggregate_stats_local
  ENGINE = Distributed('{cluster}', currentDatabase(), 'spans_aggregate_stats_local', rand());
