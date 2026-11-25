local p = ARGV[1]
local tenant = ARGV[2]
local now = clock(ARGV[3])
local queue = p .. "q:" .. tenant

local restored = 0
local queued = false
for i = 4, #ARGV, 3 do
  local job = ARGV[i]
  local attempts = tonumber(ARGV[i + 1])
  local run_after = tonumber(ARGV[i + 2])
  if redis.call("SISMEMBER", p .. "known", job) == 0 then
    redis.call("SADD", p .. "known", job)
    local current = tonumber(redis.call("HGET", p .. "attempt", job) or "0")
    if attempts > current then
      redis.call("HSET", p .. "attempt", job, attempts)
    end
    if run_after and run_after > now then
      redis.call("ZADD", p .. "delayed", num(run_after), tenant .. "|" .. job)
    else
      redis.call("RPUSH", queue, job)
      queued = true
    end
    restored = restored + 1
  end
end

if queued then
  enter_ready(p, tenant)
end
return restored
