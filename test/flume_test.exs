defmodule FlumeTest do
  use TestWithRedis

  alias Flume.Redis.Job
  alias Flume.Config

  @namespace Config.get(:namespace)

  def max_time_range do
    Flume.Queue.Backoff.calc_next_backoff(1)
    |> Flume.Support.Time.offset_from_now()
    |> Flume.Support.Time.time_to_score()
  end

  describe "enqueue/3" do
    test "enqueues a job" do
      assert {:ok, _} = Flume.enqueue("#{@namespace}:test", Worker, [1])
    end
  end

  describe "dequeue/2" do
    test "dequeues a job" do
      serialized_job = "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1089fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      Job.enqueue(Flume.Redis, "#{@namespace}:test", serialized_job)

      assert {:ok, 0} == Flume.remove_job("#{@namespace}:test", serialized_job)
    end
  end

  describe "bulk_dequeue/3" do
    test "dequeues multiple jobs and queues it to new list" do
      jobs = [
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1182fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1282fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1382fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1482fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1582fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1682fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1782fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1882fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1982fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      ]
      Enum.map(jobs, fn(job) -> Job.enqueue(Flume.Redis, "#{@namespace}:test", job) end)

      assert jobs == Flume.fetch_jobs("test", 10)
    end

    test "dequeues multiple jobs from an empty queue" do
      assert [] == Flume.fetch_jobs("test", 5)
    end
  end

  describe "retry_or_fail_job/3" do
    test "adds job to retry queue by incrementing count" do
      job = "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      Job.enqueue(Flume.Redis, "#{@namespace}:backup:test", job)

      Flume.retry_or_fail_job("test", job, "Failed")

      # make sure the job is removed from the backup queue
      assert [] == Job.fetch_all!(Flume.Redis, "#{@namespace}:backup:test")
      [[retried_job | _] | _] = Job.scheduled_jobs(Flume.Redis, ["flume_test:retry:test"], max_time_range())

      retried_job = Flume.Event.decode!(retried_job)
      assert retried_job.jid == "1082fd87-2508-4eb4-8fba-2958584a60e3"
    end

    test "adds job to dead queue if count exceeds max retries" do
      max_retries = Flume.Config.get(:max_retries) + 1
      job = "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1], \"retry_count\": #{max_retries}}"
      Job.enqueue(Flume.Redis, "#{@namespace}:backup:test", job)

      Flume.retry_or_fail_job("test", job, "Failed")

      # make sure the job is removed from the backup queue
      assert [] == Job.fetch_all!(Flume.Redis, "#{@namespace}:backup:test")

      # job will not be pushed to the retry queue
      assert [[]] == Job.scheduled_jobs(Flume.Redis, ["flume_test:retry:test"], max_time_range())
      [job_in_dead_queue] = Job.scheduled_jobs(Flume.Redis, ["flume_test:dead:test"], max_time_range())

      job_in_dead_queue = Flume.Event.decode!(job_in_dead_queue)
      assert job_in_dead_queue.jid == "1082fd87-2508-4eb4-8fba-2958584a60e3"
    end
  end

  describe "remove_retry/3" do
    test "remove job from a retry queue" do
      job = "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      Job.schedule_job(Flume.Redis, "#{@namespace}:retry:test", "1082fd87-2508-4eb4-8fba-2958584a60e3", job, DateTime.utc_now())

      assert {:ok, 1} == Flume.remove_retry("test", job)

      assert [[]] = Job.scheduled_jobs(Flume.Redis, ["flume_test:retry:test"], max_time_range())
    end
  end
end
