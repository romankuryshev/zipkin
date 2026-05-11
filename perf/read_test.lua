local services = {
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

local function random_operation()
  return OPERATIONS[math.random(1, #OPERATIONS)]
end

local idx = 0

local LOOKBACK_MS = 60 * 10 * 1000
local TIME_SPAN_MS = 30 * 24 * 60 * 60 * 1000

request = function()
  idx = (idx + 1) % #services
  local svc = services[idx + 1]

  local now = os.time() * 1000

  local endTs = now - math.random(0, TIME_SPAN_MS)

  local lookback = LOOKBACK_MS + math.random(-10 * 1000, 10 * 1000)

  local spanName = random_operation()

  if lookback < 10 * 1000 then
    lookback = 10 * 1000
  end

  wrk.method = "GET"
  wrk.body   = nil
  wrk.headers["Content-Type"] = nil

  return wrk.format(
    nil,
    "/api/v2/traces?serviceName=" .. svc ..
    "&endTs=" .. endTs ..
    "&lookback=" .. lookback ..
    "&spanName=" .. spanName ..
    "&limit=20"
  )
end
