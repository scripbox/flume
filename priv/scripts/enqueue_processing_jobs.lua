-- Script to enqueue jobs stuck in the processing-sorted-set.
--[[
  * Fetch jobs from the processing-sorted-set whose score is older than the `current_score`.
  * If these jobs count is greater than zero then proceed.
  * Enqueue these jobs to the main queue.
  * Remove these jobs from the processing-sorted-set.
]]

local processing_sorted_set_key = KEYS[1]
local queue_key = KEYS[2]

local current_score = ARGV[1]
local limit = ARGV[2]

local jobs = redis.call('ZRANGEBYSCORE', processing_sorted_set_key, '-inf', current_score, 'LIMIT', 0, limit)
local jobs_count = table.getn(jobs)

if jobs_count > 0 then
  redis.call('RPUSH', queue_key, unpack(jobs))
  redis.call('ZREM', processing_sorted_set_key, unpack(jobs))
end

return jobs_count
