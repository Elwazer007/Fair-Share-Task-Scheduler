local p = ARGV[1]
local worker = ARGV[2]
local n = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local now = clock(ARGV[5])
local default_cost = tonumber(ARGV[6])

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
  elseif redis.call("EXISTS", p .. "lease:" .. job) == 1 then
    if redis.call("LLEN", queue) == 0 then
      redis.call("ZREM", p .. "ready", tenant)
    end
  else
    local cost = tonumber(redis.call("HGET", p .. "est", tenant) or default_cost)
    local attempt = redis.call("HINCRBY", p .. "attempt", job, 1)
    redis.call("HSET", p .. "lease:" .. job,
      "attempt", attempt,
      "worker", worker,
      "tenant", tenant,
      "claimed_at", num(now),
      "charged_until", num(now),
      "estimate_left", num(cost))
    redis.call("ZADD", p .. "leases", num(now + ttl), job)

    if score > tonumber(redis.call("GET", p .. "gvt") or "0") then
      redis.call("SET", p .. "gvt", num(score))
    end

    local next_score = score + cost / weight_of(p, tenant)
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
