defmodule FlumeTest do
  use TestWithRedis

  alias Flume.{Config, Pipeline}
  alias Flume.Redis.Job
  alias Flume.Pipeline.Event.{ProducerConsumer, Consumer, Producer}

  @namespace Config.namespace()

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
        TestWithRedis.serialized_job("TestWorker", [caller_name])
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
      Enum.each(1..4, fn _ ->
        Job.enqueue(
          "#{@namespace}:queue:#{pipeline.queue}",
          TestWithRedis.serialized_job("TestWorker")
        )
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
      jobs = TestWithRedis.serialized_jobs("Elixir.Worker", 10)
      Job.bulk_enqueue("#{@namespace}:queue:#{pipeline.queue}", jobs)

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
end
