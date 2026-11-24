local p = ARGV[1]
local job = ARGV[2]
local attempt = ARGV[3]
local outcome = ARGV[4]
local run_after = ARGV[5]
local now = clock(ARGV[6])
local default_cost = tonumber(ARGV[7])
local lease = p .. "lease:" .. job

local held = redis.call("HMGET", lease, "attempt", "tenant", "claimed_at")
if not held[1] or held[1] ~= attempt then
  return "stale"
end
local tenant = held[2]

local unused = settle(p, lease, tenant, now)
if unused > 0 then
  charge(p, tenant, -unused)
end

local runtime = math.max(0, now - tonumber(held[3]))
local est = tonumber(redis.call("HGET", p .. "est", tenant) or default_cost)
redis.call("HSET", p .. "est", tenant, num(0.8 * est + 0.2 * runtime))

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
