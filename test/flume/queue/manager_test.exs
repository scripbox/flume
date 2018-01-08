defmodule Flume.Queue.ManagerTest do
  use TestWithRedis

  alias Flume.Config
  alias Flume.Redis.Job
  alias Flume.Queue.Manager
  alias Flume.Support.Time

  @namespace Config.get(:namespace)

  describe "enqueue/4" do
    test "enqueues a job to a queue" do
      assert {:ok, _} = Manager.enqueue(@namespace, "test", Worker, [1])
    end
  end

  describe "remove_job/3" do
    test "removes a job from a queue" do
      serialized_job = "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1084fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      Job.enqueue(Flume.Redis, "#{@namespace}:test", serialized_job)

      assert 1 == Manager.remove_job(@namespace, "test", "1084fd87-2508-4eb4-8fba-2958584a60e3")
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
      Enum.map(jobs, fn(job) -> Job.enqueue(Flume.Redis, "#{@namespace}:test", job) end)

      assert jobs == Manager.fetch_jobs(@namespace, "test", 10)
    end

    test "dequeues multiple jobs from an empty queue" do
      assert [] == Manager.fetch_jobs(@namespace, "test", 5)
    end
  end

  describe "retry_or_fail_job/4" do
    test "adds job to retry queue by incrementing count" do
      job = "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"

      assert {:ok, "1082fd87-2508-4eb4-8fba-2958584a60e3"} = Manager.retry_or_fail_job(
        @namespace, "test", job, "Failed"
      )
    end
  end

  describe "remove_retry/3" do
    test "remove job from a retry queue" do
      job = "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      Manager.retry_or_fail_job(@namespace, "test", job, "Failed")

      assert 1 == Manager.remove_retry(@namespace, "test", "1082fd87-2508-4eb4-8fba-2958584a60e3")
    end
  end


  describe "remove_and_enqueue_scheduled_jobs/3" do
    test "remove and enqueue scheduled jobs" do
      Job.schedule_job(
        Flume.Redis,
        "#{@namespace}:scheduled:test",
        "1083fd87-2508-4eb4-8fba-2958584a60e3",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        DateTime.utc_now()
      )
      Job.schedule_job(
        Flume.Redis,
        "#{@namespace}:retry:test",
        "1083fd97-2508-4eb4-8fba-2958584a60e3",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd97-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        DateTime.utc_now()
      )

      assert {:ok, 2} == Manager.remove_and_enqueue_scheduled_jobs(
        @namespace,
        ["test"],
        Time.time_to_score
      )
    end
  end
end
