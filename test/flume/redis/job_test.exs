defmodule Flume.Redis.JobTest do
  use TestWithRedis

  alias Flume.Config
  alias Flume.Redis.Job

  @namespace Config.get(:namespace)
  @serialized_job "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1083fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"

  describe "enqueue/3" do
    test "enqueues a job to a queue" do
      assert {:ok, _} = Job.enqueue(Flume.Redis, "#{@namespace}:test", @serialized_job)
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

      assert jobs == Job.dequeue_bulk(Flume.Redis, "#{@namespace}:test", "#{@namespace}:backup:test", 10)
      |> Enum.map(fn({:ok, job}) -> job end)
    end

    test "dequeues multiple jobs and queues it to new list 1" do
      assert [:none, :none, :none, :none, :none] = Job.dequeue_bulk(Flume.Redis, "#{@namespace}:test", "#{@namespace}:backup:test", 5)
      |> Enum.map(fn({:ok, job}) -> job end)
    end
  end

  describe "schedule_job/5" do
    test "schedules a job" do
      assert {:ok, "1082fd87-2508-4eb4-8fba-2958584a60e3"} = Job.schedule_job(
        Flume.Redis, "#{@namespace}:test",
        "1082fd87-2508-4eb4-8fba-2958584a60e3",
        @serialized_job,
        DateTime.utc_now()
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
end
