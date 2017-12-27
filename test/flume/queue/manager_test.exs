defmodule Flume.Queue.ManagerTest do
  use TestWithRedis

  alias Flume.Config
  alias Flume.Redis.Job
  alias Flume.Queue.Manager

  @namespace Config.get(:namespace)

  describe "enqueue/4" do
    test "enqueues a job" do
      assert {:ok, _} = Manager.enqueue(@namespace, "test", Worker, [1])
    end
  end

  describe "dequeue/3" do
    test "dequeues a job" do
      serialized_job = "{\"worker\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1084fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      Job.enqueue(Flume.Redis, "#{@namespace}:test", serialized_job)

      assert 1 == Manager.dequeue(@namespace, "test", serialized_job)
    end
  end
end
