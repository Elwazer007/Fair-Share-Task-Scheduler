local CHUNK = 1000

local function num(v)
  return string.format("%.17g", v)
end

local function clock(arg)
  local now = tonumber(arg)
  if now then
    return now
  end
  local t = redis.call("TIME")
  return tonumber(t[1]) + tonumber(t[2]) / 1e6
end

local function each_chunk(items, fn)
  for i = 1, #items, CHUNK do
    fn(unpack(items, i, math.min(i + CHUNK - 1, #items)))
  end
end

local function weight_of(p, tenant)
  return tonumber(redis.call("HGET", p .. "weight", tenant) or "1")
end

local function charge(p, tenant, seconds)
  if seconds == 0 then
    return
  end
  local delta = seconds / weight_of(p, tenant)
  redis.call("HINCRBYFLOAT", p .. "vtime", tenant, num(delta))
  if redis.call("ZSCORE", p .. "ready", tenant) then
    redis.call("ZINCRBY", p .. "ready", num(delta), tenant)
  end
  redis.call("HINCRBYFLOAT", p .. "usage", tenant, num(seconds))
end

local function settle(p, lease, tenant, now)
  local held = redis.call("HMGET", lease, "charged_until", "estimate_left")
  local elapsed = math.max(0, now - tonumber(held[1]))
  local left = tonumber(held[2])
  if elapsed <= left then
    left = left - elapsed
  else
    charge(p, tenant, elapsed - left)
    left = 0
  end
  redis.call("HSET", lease, "charged_until", num(now), "estimate_left", num(left))
  return left
end

local function enter_ready(p, tenant)
  if redis.call("ZSCORE", p .. "ready", tenant) then
    return
  end
  local vtime = tonumber(redis.call("HGET", p .. "vtime", tenant) or "0")
  local gvt = tonumber(redis.call("GET", p .. "gvt") or "0")
  local score = math.max(vtime, gvt)
  redis.call("HSET", p .. "vtime", tenant, num(score))
  redis.call("ZADD", p .. "ready", num(score), tenant)
end
