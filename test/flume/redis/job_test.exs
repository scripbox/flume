defmodule Flume.Redis.JobTest do
  use TestWithRedis

  alias Flume.Config
  alias Flume.Redis.Job

  @namespace Config.get(:namespace)

  describe "enqueue/3" do
    test "enqueues a job to redis list" do
      serialized_job = "{\"worker\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1083fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      assert {:ok, _} = Job.enqueue(Flume.Redis, @namespace, "test", serialized_job)
    end
  end

  describe "dequeue/3" do
    test "dequeues a job from redis list" do
      serialized_job = "{\"worker\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      Job.enqueue(Flume.Redis, @namespace, "test", serialized_job)

      assert 1 == Job.dequeue(Flume.Redis, @namespace, "test", serialized_job)
    end
  end
end
