defmodule Flume.Queue.ManagerTest do
  use TestWithRedis

  alias Flume.Config
  alias Flume.Redis.Job
  alias Flume.Queue.Manager
  alias Flume.Support.Time

  @namespace Config.get(:namespace)

  def max_time_range do
    Flume.Queue.Backoff.calc_next_backoff(1)
    |> Flume.Support.Time.offset_from_now()
    |> Flume.Support.Time.time_to_score()
  end

  describe "enqueue/4" do
    test "enqueues a job to a queue" do
      assert {:ok, _} = Manager.enqueue(@namespace, "test", Worker, [1])
    end
  end

  describe "remove_job/3" do
    test "removes a job from a queue" do
      serialized_job = "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1084fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      Job.enqueue(Flume.Redis, "#{@namespace}:queue:test", serialized_job)

      assert {:ok, 1} == Manager.remove_job(@namespace, "test", serialized_job)
    end
  end

  describe "dequeue_bulk/3" do
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
      Enum.map(jobs, fn(job) -> Job.enqueue(Flume.Redis, "#{@namespace}:queue:test", job) end)

      assert jobs == Manager.fetch_jobs(@namespace, "test", 10)
    end

    test "dequeues multiple jobs from an empty queue" do
      assert [] == Manager.fetch_jobs(@namespace, "test", 5)
    end
  end

  describe "retry_or_fail_job/4" do
    test "adds job to retry queue by incrementing count" do
      job = "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958522a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      Job.enqueue(Flume.Redis, "#{@namespace}:queue:backup:test", job)

      Manager.retry_or_fail_job(@namespace, "test", job, "Failed")

      # make sure the job is removed from the backup queue
      assert [] == Job.fetch_all!(Flume.Redis, "#{@namespace}:queue:backup:test")
      [{"#{@namespace}:retry", [retried_job]}] = Job.scheduled_jobs(Flume.Redis, ["#{@namespace}:retry"], max_time_range())

      retried_job = Flume.Event.decode!(retried_job)
      assert retried_job.jid == "1082fd87-2508-4eb4-8fba-2958522a60e3"
    end

    test "adds job to dead queue if count exceeds max retries" do
      job = "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2959994a60e3\",\"enqueued_at\":1514367662,\"args\":[1], \"retry_count\": 0}"
      Job.enqueue(Flume.Redis, "#{@namespace}:queue:backup:test", job)

      Manager.retry_or_fail_job(@namespace, "test", job, "Failed")
      # make sure the job is removed from the backup queue
      assert [] == Job.fetch_all!(Flume.Redis, "#{@namespace}:queue:backup:test")

      Enum.map(1..Flume.Config.get(:max_retries), fn(_retry_count) ->
        [{"#{@namespace}:retry", [job_to_retry]}] = Job.scheduled_jobs(Flume.Redis, ["#{@namespace}:retry"], max_time_range())
        Manager.retry_or_fail_job(@namespace, "test", job_to_retry, "Failed")
      end)

      # job will not be pushed to the retry queue
      assert [{"#{@namespace}:retry", []}] == Job.scheduled_jobs(Flume.Redis, ["#{@namespace}:retry"], max_time_range())
      [{"#{@namespace}:dead", [job_in_dead_queue]}] = Job.scheduled_jobs(Flume.Redis, ["#{@namespace}:dead"], max_time_range())

      job_in_dead_queue = Flume.Event.decode!(job_in_dead_queue)
      assert job_in_dead_queue.jid == "1082fd87-2508-4eb4-8fba-2959994a60e3"
    end
  end

  describe "remove_retry/3" do
    test "remove job from a retry queue" do
      queue = "#{@namespace}:retry"
      job = "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      Job.schedule_job(Flume.Redis, queue, DateTime.utc_now(), job)

      assert {:ok, 1} == Manager.remove_retry(@namespace, job)
      assert [{^queue, []}] = Job.scheduled_jobs(Flume.Redis, [queue], max_time_range())
    end
  end

  describe "remove_and_enqueue_scheduled_jobs/3" do
    test "remove and enqueue scheduled jobs" do
      Job.schedule_job(
        Flume.Redis,
        "#{@namespace}:scheduled",
        DateTime.utc_now(),
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      )
      Job.schedule_job(
        Flume.Redis,
        "#{@namespace}:retry",
        DateTime.utc_now(),
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd97-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      )

      queue = "#{@namespace}:queue:test"

      assert {:ok, 2} == Manager.remove_and_enqueue_scheduled_jobs(
        @namespace,
        Time.time_to_score
      )
      jobs = [
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd97-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      ]
      assert jobs == Job.fetch_all!(Flume.Redis, queue)
    end
  end
end
