local counter = 0

function setup(thread)
  counter = counter + 1
  thread:set("id", counter)
end

function init(args)
  math.randomseed(os.time() + id * 1000)
end

local SERVICES = {
  "gateway", "auth", "backend", "db", "cache", "payments", "users", "orders",
  "inventory", "search", "recommendation", "email", "notification", "billing",
  "analytics", "logging", "metrics", "api", "gateway-v2", "auth-v2",
  "profile", "media", "cdn", "upload", "download", "worker", "scheduler",
  "queue", "kafka", "rabbitmq", "redis-cache", "mysql-db", "postgres-db",
  "mongo-db", "elastic", "ai-service", "ml-inference", "fraud-detection",
  "geo-service", "feature-flag", "config-service", "rate-limiter"
}

local OPERATIONS = {
  "GET/api",
  "POST auth",
  "GET/data",
  "PUT/profile",
  "DELETE/item",
  "POST/orders",
  "GET/search",
  "GET/recommendations",
  "POST/email/send",
  "GET/metrics",
  "POST/login",
  "POST/logout"
}

local function random_service()
  return SERVICES[math.random(1, #SERVICES)]
end

local function random_operation()
  return OPERATIONS[math.random(1, #OPERATIONS)]
end

local MONTH_SECONDS = 30 * 24 * 60 * 60

-- фиксируем "конец окна" как текущее время
local TIME_END = os.time()
local TIME_START = TIME_END - MONTH_SECONDS

local function random_time_micros()
  local t = math.random(TIME_START, TIME_END)
  return t * 1000000 + math.random(0, 999999)
end

local function random_hex(len)
  local res = ""
  for i = 1, len do
    res = res .. string.format("%x", math.random(0, 15))
  end
  return res
end

local function random_ip()
  return string.format("%d.%d.%d.%d",
    math.random(1,255),
    math.random(0,255),
    math.random(0,255),
    math.random(1,254))
end

local function maybe_error()
  if math.random() < 0.1 then
    return '"error": "timeout",'
  end
  return ''
end

local TRACES_PER_REQUEST = 4

local LAT = {
  gateway = {1000, 4000},
  auth    = {500, 2000},
  backend = {1000, 5000},
  db      = {500, 3000},
  cache   = {100, 800},
  external= {2000, 7000}
}

local function rand_latency(range)
  return math.random(range[1], range[2])
end

local function span(json)
  return json
end

local function generate_trace()
  local spans = {}
  local traceId = random_hex(32)
  local client_ip = random_ip()
  local timestamp = random_time_micros()

  local gateway_id = random_hex(16)

  table.insert(spans, span(string.format([[
{
  "id": "%s",
  "traceId": "%s",
  "name": "%s",
  "timestamp": %d,
  "duration": %d,
  "kind": "SERVER",
  "localEndpoint": {
    "serviceName": "%s",
    "ipv4": "10.0.0.1",
    "port": 8080
  },
  "remoteEndpoint": {
    "ipv4": "%s"
  },
  "tags": {
    "http.method": "GET",
    "http.path": "/api",
    "http.status_code": "200"
  }
}]],
    gateway_id,
    traceId,
    random_operation(),
    timestamp,
    rand_latency(LAT.gateway),
    random_service(),
    client_ip
  )))

  local auth_id = random_hex(16)

  -- auth client
  table.insert(spans, span(string.format([[
{
  "id": "%s",
  "traceId": "%s",
  "parentId": "%s",
  "name": "%s",
  "timestamp": %d,
  "duration": %d,
  "kind": "CLIENT",
  "localEndpoint": {
    "serviceName": "%s"
  },
  "remoteEndpoint": {
    "serviceName": "%s",
    "ipv4": "10.0.0.3",
    "port": 8081
  }
}]],
    auth_id,
    traceId,
    gateway_id,
    random_operation(),
    timestamp,
    rand_latency(LAT.auth),
    random_service(),
    random_service()
  )))

  local auth_srv_id = random_hex(16)

  -- auth server
  table.insert(spans, span(string.format([[
{
  "id": "%s",
  "traceId": "%s",
  "parentId": "%s",
  "name": "%s",
  "timestamp": %d,
  "duration": %d,
  "kind": "SERVER",
  "localEndpoint": {
    "serviceName": "%s",
    "ipv4": "10.0.0.3",
    "port": 8081
  },
  "tags": {
    "auth.success": "true"
  }
}]],
    auth_srv_id,
    traceId,
    auth_id,
    random_operation(),
    timestamp,
    rand_latency(LAT.auth),
    random_service()
  )))

  local backend_id = random_hex(16)

  -- backend client
  table.insert(spans, span(string.format([[
{
  "id": "%s",
  "traceId": "%s",
  "parentId": "%s",
  "name": "%s",
  "timestamp": %d,
  "duration": %d,
  "kind": "CLIENT",
  "localEndpoint": {
    "serviceName": "%s"
  },
  "remoteEndpoint": {
    "serviceName": "%s",
    "ipv4": "10.0.0.2",
    "port": 9000
  }
}]],
    backend_id,
    traceId,
    gateway_id,
    random_operation(),
    timestamp,
    rand_latency(LAT.backend),
    random_service(),
    random_service()
  )))

  local backend_srv_id = random_hex(16)

  table.insert(spans, span(string.format([[
{
  "id": "%s",
  "traceId": "%s",
  "parentId": "%s",
  "name": "%s",
  "timestamp": %d,
  "duration": %d,
  "kind": "SERVER",
  "localEndpoint": {
    "serviceName": "%s",
    "ipv4": "10.0.0.2",
    "port": 9000
  }
}]],
    backend_srv_id,
    traceId,
    backend_id,
    random_operation(),
    timestamp,
    rand_latency(LAT.backend),
    random_service()
  )))

  return spans
end

request = function()
  local all = {}

  for i = 1, TRACES_PER_REQUEST do
    local t = generate_trace()
    for _, s in ipairs(t) do
      table.insert(all, s)
    end
  end

  local body = "[" .. table.concat(all, ",") .. "]"

  wrk.method = "POST"
  wrk.body   = body
  wrk.headers["Content-Type"] = "application/json"

  return wrk.format(nil, "/api/v2/spans")
end
