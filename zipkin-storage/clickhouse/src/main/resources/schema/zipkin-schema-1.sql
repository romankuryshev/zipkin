CREATE TABLE IF NOT EXISTS service_operation_names
(
  service_name   LowCardinality(String),
  operation_name LowCardinality(String)
) ENGINE = ReplacingMergeTree
    ORDER BY (service_name, operation_name);

CREATE TABLE IF NOT EXISTS dependencies
(
  local_service_name  String,
  remote_service_name String
) ENGINE = ReplacingMergeTree
    ORDER BY (local_service_name, remote_service_name);

CREATE TABLE IF NOT EXISTS spans
(
  trace_id        UInt64,
  trace_id_high   UInt64 DEFAULT 0,
  parent_id       Nullable(UInt64),
  span_id         UInt64,
  kind            LowCardinality(String),
  name            LowCardinality(String),
  timestamp       DATETIME64(6),
  duration        DATETIME64(6),
  local_endpoint  Nested(service_name LowCardinality(String),
                    ipv4 Nullable(IPv4),
                    ipv6 Nullable(IPv6),
                    port Nullable(UInt16)),
  nested_endpoint Nested(service_name LowCardinality(String),
                    ipv4 Nullable(IPv4),
                    ipv6 Nullable(IPv6),
                    port Nullable(UInt16)),
  Array           Nested(timestamp DateTime64(6),
                    value String),
  tags            Map(String, String),
  status_code     LowCardinality(String)
) ENGINE = MergeTree()
    PARTITION BY toDate(timestamp)
    ORDER BY (trace_id, span_id)
    SETTINGS index_granularity = 8192;

