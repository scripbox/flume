local jobs = {}
local n_jobs = tonumber(ARGV[1])
local n = 0
local len = redis.call('LLEN', KEYS[1])

if len > 0 then
  repeat
    local job = redis.call('RPOPLPUSH', KEYS[1], KEYS[2])
    if job then
      n = n + 1
      redis.call('ZADD', KEYS[3], ARGV[2], job)
      table.insert(jobs, job)
    else
      break
    end
  until n == n_jobs
end

return jobs
