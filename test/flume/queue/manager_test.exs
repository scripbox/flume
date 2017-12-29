defmodule Flume.Queue.ManagerTest do
  use TestWithRedis

  alias Flume.Config
  alias Flume.Redis.Job
  alias Flume.Queue.Manager

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

      assert 1 == Manager.remove_job(@namespace, "test", serialized_job)
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

      assert jobs == Manager.fetch_jobs(@namespace, "test", 10) |> Enum.map(fn({:ok, job}) -> job end)
    end

    test "dequeues multiple jobs and queues it to new list 1" do
      assert [:none, :none, :none, :none, :none] = Manager.fetch_jobs(@namespace, "test", 5)
      |> Enum.map(fn({:ok, job}) -> job end)
    end
  end

  describe "retry_job/4" do
    test "adds job to retry queue by incrementing count" do
      job = "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"

      assert {:ok, "1082fd87-2508-4eb4-8fba-2958584a60e3"} = Manager.retry_or_fail_job(
        @namespace, "test", job, "Failed"
      )
    end
  end
end
