local p = ARGV[1]
local now = clock(ARGV[2])
local limit = tonumber(ARGV[3])

local found = {}
local jobs = redis.call("ZRANGEBYSCORE", p .. "leases", "-inf", num(now), "LIMIT", 0, limit)
for _, job in ipairs(jobs) do
  local attempt = redis.call("HGET", p .. "lease:" .. job, "attempt")
  if attempt then
    found[#found + 1] = {job, attempt}
  else
    redis.call("ZREM", p .. "leases", job)
  end
end
return found
