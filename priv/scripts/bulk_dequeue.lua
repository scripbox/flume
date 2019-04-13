-- Script to fetch jobs from queue in bulk.
--[[
  * Fetch jobs from the main queue equal to the `size`.
  * Add these jobs to the processing-sorted-set with the current-score.
  * If the previous step is successful, remove these jobs from the main queue.
]]

local queue_key = KEYS[1]
local processing_sorted_set_key = KEYS[2]

local size = tonumber(ARGV[1])
local current_score = tostring(ARGV[2])

local jobs = redis.call('LRANGE', queue_key, 0, size - 1)
local jobs_count = table.getn(jobs)

if jobs_count > 0 then
  local jobs_with_score = {}

  for i = 1, jobs_count do
    local job = jobs[i]

    table.insert(jobs_with_score, current_score)
    table.insert(jobs_with_score, job)
  end

  redis.call('ZADD', processing_sorted_set_key, unpack(jobs_with_score))
  redis.call('LTRIM', queue_key, size, -1)
end

return jobs
