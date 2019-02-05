local jobs = {}
local n = 0

local queue_key = KEYS[1]
local backup_queue_key = KEYS[2]
local backup_sorted_set_key = KEYS[3]
local limit_sorted_set_key = KEYS[4]

local jobs_count = tonumber(ARGV[1])
local max_count = tonumber(ARGV[2])
local previous_score = tostring(ARGV[3])
local current_score = tostring(ARGV[4])

-- Time complexity O(1)
local queue_count = redis.call('LLEN', queue_key)

-- Remove old keys < (previous_score - 1s) from the limit-sorted-set
local min_score = tonumber(previous_score) - 1
redis.call('ZREMRANGEBYSCORE', limit_sorted_set_key, '-inf', min_score)

-- Time complexity O(log(N))
local processed_count = redis.call('ZCOUNT', limit_sorted_set_key, previous_score, current_score)

if (queue_count > 0 and processed_count < max_count) then
  -- remaining jobs count in the current time-window
  local remaining_count = max_count - processed_count

  if remaining_count < jobs_count then
    jobs_count = remaining_count
  end

  repeat
    local job = redis.call('RPOPLPUSH', queue_key, backup_queue_key)
    if job then
      n = n + 1
      table.insert(jobs, job)
    else
      break
    end
  until n == jobs_count

  local jobs_with_score = {}

  for i = 1, #jobs do
    local job = jobs[i]

    if job then
      table.insert(jobs_with_score, current_score)
      table.insert(jobs_with_score, job)
    end
  end

  redis.call('ZADD', backup_sorted_set_key, unpack(jobs_with_score))
  redis.call('ZADD', limit_sorted_set_key, unpack(jobs_with_score))
end

return jobs
