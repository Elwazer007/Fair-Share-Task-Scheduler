local p = ARGV[1]
local now = clock(ARGV[2])
local limit = tonumber(ARGV[3])

local due = redis.call("ZRANGEBYSCORE", p .. "delayed", "-inf", num(now), "LIMIT", 0, limit)
local tenants = {}
local moved = 0
for i = #due, 1, -1 do
  local member = due[i]
  redis.call("ZREM", p .. "delayed", member)
  local sep = string.find(member, "|", 1, true)
  local tenant = string.sub(member, 1, sep - 1)
  local job = string.sub(member, sep + 1)
  if redis.call("SISMEMBER", p .. "known", job) == 1 then
    redis.call("LPUSH", p .. "q:" .. tenant, job)
    tenants[tenant] = true
    moved = moved + 1
  end
end

for tenant in pairs(tenants) do
  enter_ready(p, tenant)
end
return moved
