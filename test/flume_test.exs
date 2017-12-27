defmodule FlumeTest do
  use TestWithRedis

  alias Flume.Redis.Job
  alias Flume.Config

  @namespace Config.get(:namespace)

  describe "enqueue/3" do
    test "enqueues a job" do
      assert {:ok, _} = Flume.enqueue("#{@namespace}:test", Worker, [1])
    end
  end

  describe "dequeue/2" do
    test "dequeues a job" do
      serialized_job = "{\"worker\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1089fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      Job.enqueue(Flume.Redis, @namespace, "test", serialized_job)

      assert 0 == Flume.dequeue("#{@namespace}:test", serialized_job)
    end
  end
end
