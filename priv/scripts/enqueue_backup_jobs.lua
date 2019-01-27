local sorted_set_key = KEYS[1]
local queue_key = KEYS[2]
local backup_queue_key = KEYS[3]

local current_score = ARGV[1]

local jobs = redis.call('ZRANGEBYSCORE', sorted_set_key, '-inf', current_score)
local jobs_size = redis.call('ZCOUNT', sorted_set_key, '-inf', current_score)

if jobs_size > 0 then
  for i = 1, #jobs do
    local job = jobs[i]

    redis.call('ZREM', sorted_set_key, job)

    local count = redis.call('LREM', backup_queue_key, 1, job)
    if count == 1 then
      redis.call('LPUSH', queue_key, job)
    end
  end
end

return jobs_size
