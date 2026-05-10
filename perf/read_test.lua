local services = {"gateway", "auth", "backend"}
local idx = 0

request = function()
  idx = (idx + 1) % #services
  local svc = services[idx + 1]

  wrk.method = "GET"
  wrk.body   = nil
  wrk.headers["Content-Type"] = nil

  return wrk.format(nil, "/api/v2/traces?serviceName=" .. svc .. "&limit=20")
end
