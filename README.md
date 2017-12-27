# Flume

Flume is a job processing system backed by Redis & [GenStage](https://github.com/elixir-lang/gen_stage).

### Goals -

* Isolate the execution of a queue of jobs.
* Custom concurrency for each pipeline.
* `rate-limiting` support for each pipeline (similar kind of jobs).
* Error handling and retry with exponential back-off.

### Specification :

* Store all jobs in Redis
* Configure the queues in Redis
* Define a supervised producer for each queue in Redis
* Configure the concurrency (no. of consumers) for each queue
* A producer pulls jobs from Redis via a separate supervised connection module (Redis.Connection)
* Each connection process holds its own connection to Redis
* Each connection process will pop jobs from a specific queue from Redis
* Configure the min & max demand for each kind of consumer
* Add rate-limit for each queue in Redis
* Handle error/exception in Consumer while processing a job
* Enqueue the job to the retry queue/pipeline
* Jobs should be retried with exponential backoff
* Should have a separate logger
