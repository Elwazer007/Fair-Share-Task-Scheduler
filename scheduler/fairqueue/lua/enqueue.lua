local p = ARGV[1]
local tenant = ARGV[2]
local at_head = ARGV[3] == "1"
local max_queued = tonumber(ARGV[4])
local queue = p .. "q:" .. tenant

local ids = {}
for i = 5, #ARGV do
  ids[#ids + 1] = ARGV[i]
end

local fresh = {}
for i = 1, #ids, CHUNK do
  local last = math.min(i + CHUNK - 1, #ids)
  local seen = redis.call("SMISMEMBER", p .. "known", unpack(ids, i, last))
  for j = i, last do
    if seen[j - i + 1] == 0 then
      fresh[#fresh + 1] = ids[j]
    end
  end
end

if #fresh == 0 then
  return 0
end

if max_queued > 0 and redis.call("LLEN", queue) + #fresh > max_queued then
  return -1
end

each_chunk(fresh, function(...)
  redis.call("SADD", p .. "known", ...)
end)

if at_head then
  for i = #fresh, 1, -1 do
    redis.call("LPUSH", queue, fresh[i])
  end
else
  each_chunk(fresh, function(...)
    redis.call("RPUSH", queue, ...)
  end)
end

enter_ready(p, tenant)
return #fresh
