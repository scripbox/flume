defmodule FlumeTest do
  use TestWithRedis

  alias Flume.{Config, Pipeline}
  alias Flume.Redis.Job
  alias Flume.Support.Time, as: TimeExtension
  alias Flume.Queue.Backoff
  alias Flume.Pipeline.Event.{ProducerConsumer, Consumer, Producer}

  @namespace Config.namespace()

  describe "enqueue/3" do
    test "enqueues a job" do
      assert {:ok, _} = Flume.enqueue("#{@namespace}:test", Worker, [1])
    end
  end

  describe "enqueue/4" do
    test "enqueues a job" do
      assert {:ok, _} = Flume.enqueue("#{@namespace}:test", Worker, "process", [1])
    end
  end

  describe "enqueue_in/3" do
    test "enqueues a job at a scheduled time" do
      assert {:ok, _} = Flume.enqueue_in("#{@namespace}:test", 10, Worker, [1])
    end
  end

  describe "enqueue_in/4" do
    test "enqueues a job at a scheduled time" do
      assert {:ok, _} = Flume.enqueue_in("#{@namespace}:test", 10, Worker, "process", [1])
    end
  end

  describe "dequeue/2" do
    test "dequeues a job" do
      serialized_job =
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1089fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"

      Job.enqueue("#{@namespace}:test", serialized_job)

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

      Enum.map(jobs, fn job -> Job.enqueue("#{@namespace}:queue:test", job) end)

      assert {:ok, jobs} == Flume.fetch_jobs("test", 10)
    end

    test "dequeues multiple jobs from an empty queue" do
      assert {:ok, []} == Flume.fetch_jobs("test", 5)
    end
  end

  describe "retry_or_fail_job/3" do
    test "adds job to retry queue by incrementing count" do
      job =
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"

      Job.enqueue("#{@namespace}:queue:backup:test", job)

      Flume.retry_or_fail_job("test", job, "Failed")

      # make sure the job is removed from the backup queue
      assert [] == Job.fetch_all!("#{@namespace}:queue:backup:test")

      {:ok, [{"#{@namespace}:retry", [retried_job | _]}]} =
        Job.scheduled_jobs(["#{@namespace}:retry"], max_time_range())

      retried_job = Flume.Event.decode!(retried_job)
      assert retried_job.jid == "1082fd87-2508-4eb4-8fba-2958584a60e3"
    end

    test "adds job to dead queue if count exceeds max retries" do
      max_retries = Config.max_retries() + 1

      job =
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1], \"retry_count\": #{
          max_retries
        }}"

      Job.enqueue("#{@namespace}:backup:test", job)

      Flume.retry_or_fail_job("test", job, "Failed")

      # make sure the job is removed from the backup queue
      assert [] == Job.fetch_all!("#{@namespace}:queue:backup:test")

      # job will not be pushed to the retry queue
      assert {:ok, [{"#{@namespace}:retry", []}]} ==
               Job.scheduled_jobs(["#{@namespace}:retry"], max_time_range())

      {:ok, [{_, [job_in_dead_queue]}]} =
        Job.scheduled_jobs(["#{@namespace}:dead"], max_time_range())

      job_in_dead_queue = Flume.Event.decode!(job_in_dead_queue)
      assert job_in_dead_queue.jid == "1082fd87-2508-4eb4-8fba-2958584a60e3"
    end
  end

  describe "remove_retry/3" do
    test "remove job from a retry queue" do
      queue = "#{@namespace}:retry"

      job =
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"

      Job.schedule_job(queue, TimeExtension.unix_seconds(), job)

      assert {:ok, 1} == Flume.remove_retry(job)

      assert {:ok, [{^queue, []}]} = Job.scheduled_jobs([queue], max_time_range())
    end
  end

  describe "remove_backup/3" do
    test "remove job from a backup queue" do
      job =
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"

      Job.enqueue("#{@namespace}:queue:backup:test", job)

      assert {:ok, 1} == Flume.remove_backup("test", job)
    end
  end

  describe "pending_jobs_count/0" do
    defmodule TestWorker do
      def start_link(pipeline, event) do
        Task.start_link(__MODULE__, :run, [pipeline, event])
      end

      def run(_pipeline, event) do
        %{"args" => [caller_name]} = Jason.decode!(event)
        caller_name = caller_name |> String.to_atom()
        send(caller_name, {:hello, self()})

        receive do
          {:ack, pid} ->
            send(pid, :done)
        end
      end
    end

    test "returns pending jobs count" do
      pipeline = %Pipeline{
        name: "test_pipeline",
        queue: "test",
        max_demand: 1000
      }

      caller_name = :test_process
      Process.register(self(), caller_name)

      {:ok, producer} = Producer.start_link(pipeline)
      {:ok, _} = ProducerConsumer.start_link(pipeline)
      {:ok, _} = Consumer.start_link(pipeline, TestWorker)

      # Push events to Redis
      Job.enqueue(
        "#{@namespace}:queue:#{pipeline.queue}",
        serialized_job("TestWorker", caller_name)
      )

      receive do
        {:hello, pid} ->
          assert 1 == Flume.pending_jobs_count([:test_pipeline])
          send(pid, {:ack, caller_name})
      end

      GenStage.stop(producer)
    end
  end

  @tag :slow_test
  describe "regular pipelines" do
    test "pulls max_demand events from redis" do
      max_demand = 2
      sleep_time = 500

      pipeline = %Pipeline{
        name: "test_pipeline",
        queue: "test",
        max_demand: max_demand
      }

      caller_name = :test_process
      Process.register(self(), caller_name)

      {:ok, producer} = Producer.start_link(pipeline)
      {:ok, producer_consumer} = ProducerConsumer.start_link(pipeline)

      {:ok, _} =
        EchoConsumerWithTimestamp.start_link(%{
          upstream: producer_consumer,
          owner: caller_name,
          max_demand: max_demand,
          sleep_time: sleep_time
        })

      # Push events to Redis
      Enum.each(1..4, fn i ->
        Job.enqueue("#{@namespace}:queue:#{pipeline.queue}", i)
      end)

      assert_receive {:received, events, received_time_1}, 4_000
      assert length(events) == 2

      assert_receive {:received, events, received_time_2}, 4_000
      assert length(events) == 2

      assert received_time_2 > received_time_1

      GenStage.stop(producer)
    end
  end

  @tag :slow_test
  describe "rate-limited pipelines" do
    test "pulls max_demand events from redis within the rate-limit-scale" do
      max_demand = 10
      sleep_time = 500

      pipeline = %Pipeline{
        name: "test_pipeline",
        queue: "test",
        max_demand: max_demand,
        rate_limit_count: 2,
        rate_limit_scale: 1000
      }

      caller_name = :test_process
      Process.register(self(), caller_name)

      {:ok, producer} = Producer.start_link(pipeline)
      {:ok, producer_consumer} = ProducerConsumer.start_link(pipeline)

      {:ok, _} =
        EchoConsumerWithTimestamp.start_link(%{
          upstream: producer_consumer,
          owner: caller_name,
          max_demand: max_demand,
          sleep_time: sleep_time
        })

      # Push events to Redis
      Enum.each(1..10, fn i ->
        Job.enqueue("#{@namespace}:queue:#{pipeline.queue}", i)
      end)

      assert_receive {:received, events, received_time_1}, 4_000
      assert length(events) == 2

      assert_receive {:received, events, received_time_2}, 4_000
      assert length(events) == 2
      assert received_time_2 > received_time_1

      assert_receive {:received, events, received_time_3}, 4_000
      assert received_time_3 > received_time_2
      assert length(events) == 2

      assert_receive {:received, events, received_time_4}, 4_000
      assert received_time_4 > received_time_3
      assert length(events) == 2

      assert_receive {:received, events, received_time_5}, 4_000
      assert received_time_5 > received_time_4
      assert length(events) == 2

      GenStage.stop(producer)
    end
  end

  defp max_time_range do
    Backoff.calc_next_backoff(1)
    |> TimeExtension.offset_from_now()
    |> TimeExtension.time_to_score()
  end

  defp serialized_job(module_name, args) do
    %{
      class: module_name,
      function: "perform",
      queue: "test",
      jid: "1082fd87-2508-4eb4-8fba-2958584a60e3",
      args: [args],
      retry_count: 0,
      enqueued_at: 1_514_367_662,
      finished_at: nil,
      failed_at: nil,
      retried_at: nil,
      error_message: nil,
      error_backtrace: nil
    }
    |> Jason.encode!()
  end
end
