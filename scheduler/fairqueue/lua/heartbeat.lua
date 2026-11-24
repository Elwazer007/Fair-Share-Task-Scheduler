local p = ARGV[1]
local worker = ARGV[2]
local ttl = tonumber(ARGV[3])
local now = clock(ARGV[4])

local lost = {}
for i = 5, #ARGV, 2 do
  local job = ARGV[i]
  local lease = p .. "lease:" .. job
  local held = redis.call("HMGET", lease, "attempt", "worker", "tenant")
  if held[1] == ARGV[i + 1] and held[2] == worker then
    settle(p, lease, held[3], now)
    redis.call("ZADD", p .. "leases", num(now + ttl), job)
  else
    lost[#lost + 1] = job
  end
end

return lost
