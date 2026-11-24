local p = ARGV[1]
local tenant = ARGV[2]
local at_head = ARGV[3] == "1"
local queue = p .. "q:" .. tenant

local fresh = {}
for i = 4, #ARGV do
  if redis.call("SADD", p .. "known", ARGV[i]) == 1 then
    fresh[#fresh + 1] = ARGV[i]
  end
end

if #fresh == 0 then
  return 0
end

if at_head then
  for i = #fresh, 1, -1 do
    redis.call("LPUSH", queue, fresh[i])
  end
else
  redis.call("RPUSH", queue, unpack(fresh))
end

if not redis.call("ZSCORE", p .. "ready", tenant) then
  local vtime = tonumber(redis.call("HGET", p .. "vtime", tenant) or "0")
  redis.call("ZADD", p .. "ready", string.format("%.17g", vtime), tenant)
end

return #fresh
