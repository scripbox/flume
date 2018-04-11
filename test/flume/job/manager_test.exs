defmodule Flume.Job.ManagerTest do
  use TestWithRedis

  alias Flume.{Config, Event, Job}
  alias Flume.Redis.Job, as: RedisJob
  alias Flume.Redis.Client
  alias Flume.Job.Manager

  @namespace Config.get(:namespace)

  setup _manager_server do
    Manager.start_link()
    :ok
  end

  describe "monitor/2" do
    test "monitors and records when a job has started" do
      serialized_job = "{\"a\":\"hello\",\"queue\":\"test\"}"

      job = %Job{
        status: Job.started(),
        event: %Event{queue: "test", original_json: serialized_job},
        error_message: "failed"
      }

      pid = spawn(fn -> "hello" end)

      Manager.handle_cast({:monitor, pid, job}, %{})
      Manager.handle_info({:DOWN, nil, :process, pid, :failed}, %{})
      Manager.retry_jobs()

      assert 1 == Client.zrange!("#{@namespace}:retry") |> length
    end

    test "monitors and records when a job has processed" do
      serialized_job = "{\"a\":\"hello\",\"queue\":\"test\"}"

      job = %Job{
        status: Job.processed(),
        event: %Event{queue: "test", original_json: serialized_job},
        error_message: "failed"
      }

      RedisJob.enqueue("#{@namespace}:backup:test", serialized_job)
      pid = spawn(fn -> "hello" end)

      Manager.handle_cast({:monitor, pid, job}, %{})
      Manager.handle_info({:DOWN, nil, :process, pid, :normal}, %{})
      Manager.clear_completed_jobs()

      assert [serialized_job] == RedisJob.fetch_all!("#{@namespace}:backup:test")
    end
  end
end
