-- TODO: Deprecated, remove this later!
local len = redis.call('ZCOUNT', KEYS[1], '-inf', ARGV[3])

if len > 0 then
  local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[3])
  for i = 1, #jobs do
    local job = jobs[i]
    if job then
      local decoded = cjson.decode(jobs[i])
      local queue = decoded["queue"]
      if queue then
        local queue_key = string.format('%s:queue:%s', ARGV[1], queue)
        local backup_queue_key = string.format('%s:queue:backup:%s', ARGV[1], queue)
        local enqueued_at = decoded["enqueued_at"]
        if not enqueued_at then
          decoded["enqueued_at"] = ARGV[2]
        end
        redis.call('ZREM', KEYS[1], job)
        local count = redis.call('LREM', backup_queue_key, 1, job)
        if count == 1 then
          local encoded = cjson.encode(decoded)
          redis.call('LPUSH', queue_key, encoded)
        end
      end
    else
      break
    end
  end
end

return len
