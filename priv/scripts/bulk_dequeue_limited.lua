-- Script to fetch jobs from a rate-limited queue in bulk.
--[[
  * Get jobs count of the main queue.
  * Remove entries from the limit-sorted-set which are older than the `min_score`.
  * Count entries in the limit-sorted-set which are between the `previous_score` & `current_score`.
  * If queue-count is greater than zero and processed-count is less than max-count then proceed.
  * Calculate remaining-count for jobs which can be processed.
  * Fetch jobs from the main queue equal to the `size` calculated from the previous step.
  * Add these jobs to the processing-sorted-set with the current-score.
  * Add entries to the limit-sorted-set with the current-score.
  * If the previous step is successful, remove these jobs from the main queue.
]]

local jobs = {}

local queue_key = KEYS[1]
local processing_sorted_set_key = KEYS[2]
local limit_sorted_set_key = KEYS[3]

local size = tonumber(ARGV[1])
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

  if remaining_count < size then
    size = remaining_count
  end

  jobs = redis.call('LRANGE', queue_key, 0, size - 1)
  local jobs_count = table.getn(jobs)

  if jobs_count > 0 then
    local jobs_with_score = {}

    for i = 1, jobs_count do
      local job = jobs[i]

      table.insert(jobs_with_score, current_score)
      table.insert(jobs_with_score, job)
    end

    redis.call('ZADD', processing_sorted_set_key, unpack(jobs_with_score))
    redis.call('ZADD', limit_sorted_set_key, unpack(jobs_with_score))
    redis.call('LTRIM', queue_key, size, -1)
  end
end

return jobs
