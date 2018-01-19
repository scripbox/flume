defmodule Flume.Redis.JobTest do
  use TestWithRedis

  alias Flume.Config
  alias Flume.Redis.Job
  alias Flume.Support.Time

  @namespace Config.get(:namespace)
  @serialized_job "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1083fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"

  describe "enqueue/3" do
    test "enqueues a job to a queue" do
      assert {:ok, _} = Job.enqueue(Flume.Redis, "#{@namespace}:test", @serialized_job)
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

      assert jobs == Job.bulk_dequeue(
        Flume.Redis,
        "#{@namespace}:test",
        "#{@namespace}:backup:test",
        10
      )
    end

    test "dequeues multiple jobs from an empty queue" do
      assert [] = Job.bulk_dequeue(Flume.Redis, "#{@namespace}:test", "#{@namespace}:backup:test", 5)
    end
  end

  describe "schedule_job/5" do
    test "schedules a job" do
      assert {:ok, 1} = Job.schedule_job(
        Flume.Redis, "#{@namespace}:test",
        DateTime.utc_now() |> Time.unix_seconds,
        @serialized_job
      )
    end
  end

  describe "remove_job/3" do
    test "removes a job from a queue" do
      Job.enqueue(Flume.Redis, "#{@namespace}:test", @serialized_job)

      assert 1 == Job.remove_job!(Flume.Redis, "#{@namespace}:test", @serialized_job)
    end
  end

  describe "remove_retry_job/3" do
    test "removes a job from a retry queue" do
      Job.fail_job!(Flume.Redis, "#{@namespace}:test", @serialized_job)

      assert 1 == Job.remove_scheduled_job!(Flume.Redis, "#{@namespace}:test", @serialized_job)
    end
  end

  describe "fail_job/3" do
    test "adds a job to retry queue" do
      assert 1 == Job.fail_job!(Flume.Redis, "#{@namespace}:test", @serialized_job)
    end
  end

  describe "fetch_all/2" do
    test "fetches all jobs from a list" do
      Job.enqueue(Flume.Redis, "#{@namespace}:test", @serialized_job)

      assert [@serialized_job] == Job.fetch_all!(Flume.Redis, "#{@namespace}:test")
    end
  end

  describe "scheduled_job/3" do
    test "returns scheduled jobs" do
      {:ok, _jid} = Job.schedule_job(
        Flume.Redis,
        "#{@namespace}:test",
        DateTime.utc_now() |> Time.unix_seconds,
        @serialized_job
      )

      jobs = Job.scheduled_jobs(
        Flume.Redis, ["#{@namespace}:test"], Time.time_to_score
      )

      assert [{"#{@namespace}:test", [@serialized_job]}] == jobs
    end
  end
end
