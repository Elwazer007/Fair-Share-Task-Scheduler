local p = ARGV[1]
local worker = ARGV[2]
local n = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local now = tonumber(ARGV[5])
local cost = tonumber(ARGV[6])

if not now then
  local t = redis.call("TIME")
  now = tonumber(t[1]) + tonumber(t[2]) / 1e6
end

local function num(v)
  return string.format("%.17g", v)
end

local claimed = {}
local guard = n * 2 + 10

while #claimed < n and guard > 0 do
  guard = guard - 1
  local head = redis.call("ZRANGE", p .. "ready", 0, 0, "WITHSCORES")
  if #head == 0 then
    break
  end
  local tenant = head[1]
  local score = tonumber(head[2])
  local queue = p .. "q:" .. tenant
  local job = redis.call("LPOP", queue)

  if not job then
    redis.call("ZREM", p .. "ready", tenant)
  else
    local attempt = redis.call("HINCRBY", p .. "attempt", job, 1)
    redis.call("HSET", p .. "lease:" .. job,
      "attempt", attempt,
      "worker", worker,
      "tenant", tenant,
      "claimed_at", num(now),
      "charged_until", num(now),
      "estimate_left", num(cost))
    redis.call("ZADD", p .. "leases", num(now + ttl), job)

    local weight = tonumber(redis.call("HGET", p .. "weight", tenant) or "1")
    local next_score = score + cost / weight
    redis.call("HSET", p .. "vtime", tenant, num(next_score))
    if redis.call("LLEN", queue) == 0 then
      redis.call("ZREM", p .. "ready", tenant)
    else
      redis.call("ZADD", p .. "ready", num(next_score), tenant)
    end

    redis.call("HINCRBY", p .. "running", tenant, 1)
    redis.call("HINCRBYFLOAT", p .. "usage", tenant, num(cost))
    claimed[#claimed + 1] = {job, tenant, attempt}
  end
end

return claimed
