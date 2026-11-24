local p = ARGV[1]
local job = ARGV[2]
local attempt = ARGV[3]
local outcome = ARGV[4]
local run_after = ARGV[5]
local lease = p .. "lease:" .. job

local held = redis.call("HMGET", lease, "attempt", "tenant")
if not held[1] or held[1] ~= attempt then
  return "stale"
end
local tenant = held[2]

redis.call("DEL", lease)
redis.call("ZREM", p .. "leases", job)
if tonumber(redis.call("HINCRBY", p .. "running", tenant, -1)) <= 0 then
  redis.call("HDEL", p .. "running", tenant)
end

if outcome == "retry" then
  redis.call("ZADD", p .. "delayed", run_after, tenant .. "|" .. job)
else
  redis.call("SREM", p .. "known", job)
  redis.call("HDEL", p .. "attempt", job)
end

return "ok"
