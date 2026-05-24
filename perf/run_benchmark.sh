#!/usr/bin/env bash
set -euo pipefail


BACKEND=${1:-}
if [[ -z "$BACKEND" ]]; then
  echo "Usage: $0 <mem|elasticsearch|clickhouse|cassandra|mysql>"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_DIR="${REPO_ROOT}/docker/examples"
JAR=$(ls "${REPO_ROOT}/zipkin-server/target/zipkin-server-"*exec.jar 2>/dev/null | head -1)

if [[ -z "$JAR" ]]; then
  echo "ERROR: zipkin-server exec.jar not found. Build first:"
  echo "  ./mvnw -q --batch-mode -DskipTests --also-make -pl zipkin-server clean install"
  exit 1
fi

ZIPKIN_URL="http://localhost:9411"
RESULTS_DIR="${SCRIPT_DIR}/results/${BACKEND}"
mkdir -p "${RESULTS_DIR}"

ZIPKIN_PID=""

# ---------------------------------------------------------------------------
cleanup() {
  echo ""
  echo "--- Stopping Zipkin ---"
  if [[ -n "$ZIPKIN_PID" ]] && kill -0 "$ZIPKIN_PID" 2>/dev/null; then
    kill "$ZIPKIN_PID"
    wait "$ZIPKIN_PID" 2>/dev/null || true
  fi

  echo "--- Stopping infrastructure ---"
  case "$BACKEND" in
    elasticsearch)
      docker compose -f "${COMPOSE_DIR}/docker-compose-elasticsearch.yml" stop storage 2>/dev/null || true
#      docker compose -f "${COMPOSE_DIR}/docker-compose-elasticsearch.yml" rm -f storage 2>/dev/null || true
      ;;
    clickhouse)
      docker compose -f "${COMPOSE_DIR}/docker-compose-clickhouse.yaml" down 2>/dev/null || true
      ;;
    cassandra)
      docker compose -f "${COMPOSE_DIR}/docker-compose-cassandra.yml" stop storage 2>/dev/null || true
      docker compose -f "${COMPOSE_DIR}/docker-compose-cassandra.yml" rm -f storage 2>/dev/null || true
      ;;
    mysql)
      docker compose -f "${COMPOSE_DIR}/docker-compose-mysql.yml" stop storage 2>/dev/null || true
      docker compose -f "${COMPOSE_DIR}/docker-compose-mysql.yml" rm -f storage 2>/dev/null || true
      ;;
  esac
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
start_infra() {
  case "$BACKEND" in
    mem)
      echo "--- In-memory: no infrastructure needed ---"
      ;;
    elasticsearch)
      echo "--- Starting Elasticsearch ---"
      docker compose -f "${COMPOSE_DIR}/docker-compose-elasticsearch.yml" up -d storage
      echo -n "Waiting for Elasticsearch..."
      until curl -sf "http://localhost:9200/_cluster/health" > /dev/null 2>&1; do
        echo -n "."; sleep 3
      done
      echo " ready"
      ;;
    clickhouse)
      echo "--- Starting ClickHouse ---"
      docker compose -f "${COMPOSE_DIR}/docker-compose-clickhouse.yaml" up -d clickhouse
      echo -n "Waiting for ClickHouse..."
      until docker compose -f "${COMPOSE_DIR}/docker-compose-clickhouse.yaml" ps clickhouse \
            | grep -q "healthy"; do
        echo -n "."; sleep 3
      done
      echo " ready"
      ;;
    cassandra)
      echo "--- Starting Cassandra ---"
      docker compose -f "${COMPOSE_DIR}/docker-compose-cassandra.yml" up -d storage
      echo -n "Waiting for Cassandra..."
      until docker compose -f "${COMPOSE_DIR}/docker-compose-cassandra.yml" ps storage \
            | grep -q "healthy"; do
        echo -n "."; sleep 5
      done
      echo " ready"
      ;;
    mysql)
      echo "--- Starting MySql ---"
      docker compose -f "${COMPOSE_DIR}/docker-compose-mysql.yml" up -d storage
      echo -n "Waiting for MySql..."
      until docker compose -f "${COMPOSE_DIR}/docker-compose-mysql.yml" ps storage \
            | grep -q "healthy"; do
        echo -n "."; sleep 5
      done
      echo " ready"
      ;;
  esac
}

# ---------------------------------------------------------------------------
start_zipkin() {
  local log_file="${RESULTS_DIR}/zipkin.log"
  echo "--- Starting Zipkin with backend=${BACKEND} (log: ${log_file}) ---"

  case "$BACKEND" in
    mem)
      STORAGE_TYPE=mem \
        java -jar "${JAR}" > "${log_file}" 2>&1 &
      ;;
    elasticsearch)
      STORAGE_TYPE=elasticsearch \
      ES_HOSTS=localhost:9200 \
        java -XX:TieredStopAtLevel=1 -Dio.netty.transport.noNative=true -jar "${JAR}" > "${log_file}" 2>&1 &
      ;;
    clickhouse)
      STORAGE_TYPE=clickhouse \
      CH_HOST=localhost \
      CH_PORT=8123 \
      CH_USERNAME=zipkin \
      CH_PASSWORD=zipkin \
        java -jar "${JAR}" > "${log_file}" 2>&1 &
      ;;
    cassandra)
      STORAGE_TYPE=cassandra3 \
      CASSANDRA_CONTACT_POINTS=localhost \
      CASSANDRA_ENSURE_SCHEMA=true \
        java -jar "${JAR}" > "${log_file}" 2>&1 &
      ;;
    mysql)
      STORAGE_TYPE=mysql \
      MYSQL_HOST=localhost \
      MYSQL_USER=zipkin \
      MYSQL_PASS=zipkin \
      MYSQL_DB=zipkin \
        java -jar "${JAR}" > "${log_file}" 2>&1 &
      ;;
  esac

  ZIPKIN_PID=$!
  echo -n "Waiting for Zipkin (pid=${ZIPKIN_PID})..."
  local attempts=0
  until curl -sf "${ZIPKIN_URL}/health" > /dev/null 2>&1; do
    if ! kill -0 "$ZIPKIN_PID" 2>/dev/null; then
      echo ""
      echo "ERROR: Zipkin crashed. Check ${log_file}"
      exit 1
    fi
    echo -n "."; sleep 3
    attempts=$((attempts + 1))
    if [[ $attempts -gt 40 ]]; then
      echo ""
      echo "ERROR: Zipkin did not start in time. Check ${log_file}"
      exit 1
    fi
  done
  echo " ready"
}

# ---------------------------------------------------------------------------
run_wrk2() {
  local label=$1
  local rate=$2
  local threads=$3
  local conns=$4
  local script=$5
  local out="${RESULTS_DIR}/${label}.txt"

  echo "  wrk2: ${label} (rate=${rate} rps, threads=${threads}, conns=${conns}, 30s)"
  wrk2 -t"${threads}" -c"${conns}" -d30s -R"${rate}" --latency \
    -s "${SCRIPT_DIR}/${script}" \
    "${ZIPKIN_URL}" \
    > "${out}" 2>&1

  grep -E "Requests/sec:|50\.000%|99\.000%" "${out}" | head -5 | sed 's/^/    /'
}

# ---------------------------------------------------------------------------
echo "======================================================================"
echo "  Benchmark: ${BACKEND}"
echo "  Results:   ${RESULTS_DIR}"
echo "======================================================================"

#start_infra
start_zipkin
#
#echo ""
#echo "--- Warmup (200 rps, 20s) ---"
#wrk2 -t2 -c20 -d20s -R200 \
#  -s "${SCRIPT_DIR}/zipkin_microservices.lua" \
#  "${ZIPKIN_URL}" > /dev/null 2>&1
#
#echo ""
#echo "--- Write tests (POST /api/v2/spans) ---"
#for RATE in 200 500 1000 2000; do
#  run_wrk2 "write_${RATE}rps" "${RATE}" 4 50 "zipkin_microservices.lua"
#done
#

echo ""
echo "--- Warmup read tests---"
wrk2 -t2 -c10 -d20s -R10 \
  -s "${SCRIPT_DIR}/read_test.lua" \
    "${ZIPKIN_URL}" > /dev/null 2>&1

echo ""
echo "--- Read tests (GET /api/v2/traces) ---"
for RATE in 50 100 200; do
  run_wrk2 "read_${RATE}rps" "${RATE}" 2 10 "read_test.lua"
done

