local jobs = {}
local n = 0

local queue_key = KEYS[1]
local backup_queue_key = KEYS[2]
local sorted_set_key = KEYS[3]

local jobs_count = tonumber(ARGV[1])
local current_score = tostring(ARGV[2])

-- Time complexity O(1)
local queue_count = redis.call('LLEN', queue_key)

if queue_count > 0 then
  repeat
    local job = redis.call('RPOPLPUSH', queue_key, backup_queue_key)
    if job then
      n = n + 1
      redis.call('ZADD', sorted_set_key, current_score, job)
      table.insert(jobs, job)
    else
      break
    end
  until n == jobs_count
end

return jobs
