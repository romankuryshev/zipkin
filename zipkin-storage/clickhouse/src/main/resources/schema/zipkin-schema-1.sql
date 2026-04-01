CREATE TABLE IF NOT EXISTS service_operation_names
(
  service_name   LowCardinality(String),
  operation_name LowCardinality(String)
) ENGINE = ReplacingMergeTree
    ORDER BY (service_name, operation_name);

CREATE TABLE IF NOT EXISTS dependencies
(
  local_service_name  LowCardinality(String),
  remote_service_name LowCardinality(String)
) ENGINE = ReplacingMergeTree
    ORDER BY (local_service_name, remote_service_name);

CREATE TABLE IF NOT EXISTS spans
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
  status_code                  LowCardinality(String)
) ENGINE = MergeTree()
    PARTITION BY toDate(timestamp)
    ORDER BY (name, local_endpoint_service_name, kind)
    SETTINGS index_granularity = 8192;

ALTER TABLE spans
  ADD PROJECTION IF NOT EXISTS spans_prj_service_name
    (
    SELECT *
    ORDER BY (trace_id)
    );
