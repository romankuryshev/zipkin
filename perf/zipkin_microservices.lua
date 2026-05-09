math.randomseed(os.time())

local function random_hex(len)
  local res = ""
  for i = 1, len do
    res = res .. string.format("%x", math.random(0, 15))
  end
  return res
end

local function now_micros()
  return os.time() * 1000000 + math.random(0, 999999)
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
  local traceId = random_hex(16)

  local client_ip = random_ip()

  local gateway_id = random_hex(16)

  table.insert(spans, span(string.format([[
{
  "id": "%s",
  "traceId": "%s",
  "name": "GET /api",
  "timestamp": %d,
  "duration": %d,
  "kind": "SERVER",
  "localEndpoint": {
    "serviceName": "gateway",
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
    gateway_id, traceId, now_micros(),
    rand_latency(LAT.gateway),
    client_ip
  )))

  local auth_id = random_hex(16)

  table.insert(spans, span(string.format([[
{
  "id": "%s",
  "traceId": "%s",
  "parentId": "%s",
  "name": "POST /auth",
  "timestamp": %d,
  "duration": %d,
  "kind": "CLIENT",
  "localEndpoint": {
    "serviceName": "gateway"
  },
  "remoteEndpoint": {
    "serviceName": "auth",
    "ipv4": "10.0.0.3",
    "port": 8081
  }
}]],
    auth_id, traceId, gateway_id,
    now_micros(),
    rand_latency(LAT.auth)
  )))

  local auth_srv_id = random_hex(16)

  table.insert(spans, span(string.format([[
{
  "id": "%s",
  "traceId": "%s",
  "parentId": "%s",
  "name": "POST /auth",
  "timestamp": %d,
  "duration": %d,
  "kind": "SERVER",
  "localEndpoint": {
    "serviceName": "auth",
    "ipv4": "10.0.0.3",
    "port": 8081
  },
  "tags": {
    "auth.success": "true"
  }
}]],
    auth_srv_id, traceId, auth_id,
    now_micros(),
    rand_latency(LAT.auth)
  )))

  local backend_id = random_hex(16)

  table.insert(spans, span(string.format([[
{
  "id": "%s",
  "traceId": "%s",
  "parentId": "%s",
  "name": "GET /data",
  "timestamp": %d,
  "duration": %d,
  "kind": "CLIENT",
  "localEndpoint": {
    "serviceName": "gateway"
  },
  "remoteEndpoint": {
    "serviceName": "backend",
    "ipv4": "10.0.0.2",
    "port": 9000
  }
}]],
    backend_id, traceId, gateway_id,
    now_micros(),
    rand_latency(LAT.backend)
  )))

  -- backend SERVER
  local backend_srv_id = random_hex(16)

  table.insert(spans, span(string.format([[
{
  "id": "%s",
  "traceId": "%s",
  "parentId": "%s",
  "name": "GET /data",
  "timestamp": %d,
  "duration": %d,
  "kind": "SERVER",
  "localEndpoint": {
    "serviceName": "backend",
    "ipv4": "10.0.0.2",
    "port": 9000
  }
}]],
    backend_srv_id, traceId, backend_id,
    now_micros(),
    rand_latency(LAT.backend)
  )))

  local db_id = random_hex(16)

  table.insert(spans, span(string.format([[
{
  "id": "%s",
  "traceId": "%s",
  "parentId": "%s",
  "name": "SELECT users",
  "timestamp": %d,
  "duration": %d,
  "kind": "CLIENT",
  "localEndpoint": {
    "serviceName": "backend"
  },
  "remoteEndpoint": {
    "serviceName": "mysql",
    "ipv4": "10.0.0.10",
    "port": 3306
  },
  "tags": {
    "db.type": "sql",
    %s
    "component": "mysql"
  }
}]],
    db_id, traceId, backend_srv_id,
    now_micros(),
    rand_latency(LAT.db),
    maybe_error()
  )))

  local cache_id = random_hex(16)

  table.insert(spans, span(string.format([[
{
  "id": "%s",
  "traceId": "%s",
  "parentId": "%s",
  "name": "GET cache",
  "timestamp": %d,
  "duration": %d,
  "kind": "CLIENT",
  "localEndpoint": {
    "serviceName": "backend"
  },
  "remoteEndpoint": {
    "serviceName": "redis",
    "ipv4": "10.0.0.11",
    "port": 6379
  }
}]],
    cache_id, traceId, backend_srv_id,
    now_micros(),
    rand_latency(LAT.cache)
  )))

  local ext_id = random_hex(16)

  table.insert(spans, span(string.format([[
{
  "id": "%s",
  "traceId": "%s",
  "parentId": "%s",
  "name": "GET external",
  "timestamp": %d,
  "duration": %d,
  "kind": "CLIENT",
  "localEndpoint": {
    "serviceName": "backend"
  },
  "remoteEndpoint": {
    "serviceName": "payments",
    "ipv4": "52.12.34.56",
    "port": 443
  }
}]],
    ext_id, traceId, backend_srv_id,
    now_micros(),
    rand_latency(LAT.external)
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
