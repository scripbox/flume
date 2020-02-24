defmodule FlumeTest do
  use Flume.TestWithRedis

  import Flume.Mock

  alias Flume.Redis.Job
  alias Flume.{Redis, Config, Pipeline, JobFactory}
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
        JobFactory.generate("TestWorker", [caller_name])
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

      # Push events to Redis
      Enum.each(1..4, fn _ ->
        Job.enqueue(
          "#{@namespace}:queue:#{pipeline.queue}",
          JobFactory.generate("TestWorker")
        )
      end)

      {:ok, producer} = Producer.start_link(pipeline)
      {:ok, producer_consumer} = ProducerConsumer.start_link(pipeline)

      {:ok, _} =
        EchoConsumerWithTimestamp.start_link(%{
          upstream: producer_consumer,
          owner: caller_name,
          max_demand: max_demand,
          sleep_time: sleep_time
        })

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
      jobs = JobFactory.generate_jobs("Elixir.Worker", 10)
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

  describe "enqueue/4" do
    test "mock works" do
      with_flume_mock do
        Flume.enqueue(:test, List, :last, [[1]])

        assert_receive %{
          queue: :test,
          worker: List,
          function_name: :last,
          args: [[1]]
        }
      end
    end
  end

  describe "enqueue_in/5" do
    test "mock works" do
      with_flume_mock do
        Flume.enqueue_in(:test, 10, List, :last, [[1]])

        assert_receive %{
          schedule_in: 10,
          queue: :test,
          worker: List,
          function_name: :last,
          args: [[1]]
        }
      end
    end
  end

  describe "bulk_enqueue/4" do
    test "mock works" do
      with_flume_mock do
        Flume.bulk_enqueue(
          :test,
          [
            [List, "last", [[1]]],
            [List, "last", [[2, 3]]]
          ]
        )

        assert_receive %{
          queue: :test,
          jobs: [
            [List, "last", [[1]]],
            [List, "last", [[2, 3]]]
          ]
        }
      end
    end
  end

  test "pipelines/0" do
    assert Flume.pipelines() == [%{name: "default_pipeline", queue: "default", max_demand: 1000}]
  end

  describe "pause_all/1" do
    test "pauses all the pipelines temporarily" do
      assert Flume.pause_all(async: false, temporary: true) == [:ok]

      Enum.each(Flume.Config.pipeline_names(), fn pipeline_name ->
        process_name = Pipeline.Event.Producer.process_name(pipeline_name)

        assert match?(
                 %{
                   state: %{
                     paused: true
                   }
                 },
                 :sys.get_state(process_name)
               )

        assert Redis.Client.get!("flume_test:pipeline:#{pipeline_name}:paused") == nil
      end)
    end

    test "pauses all the pipelines permanently" do
      assert Flume.pause_all(async: false, temporary: false) == [:ok]

      Enum.each(Flume.Config.pipeline_names(), fn pipeline_name ->
        process_name = Pipeline.Event.Producer.process_name(pipeline_name)

        assert match?(
                 %{
                   state: %{
                     paused: true
                   }
                 },
                 :sys.get_state(process_name)
               )

        assert Redis.Client.get!("flume_test:pipeline:#{pipeline_name}:paused") == "true"
      end)
    end
  end

  describe "resume_all/2" do
    test "resumes all the pipelines temporarily" do
      assert Flume.pause_all(async: false, temporary: false) == [:ok]
      assert Flume.resume_all(async: false, temporary: true) == [:ok]

      Enum.each(Flume.Config.pipeline_names(), fn pipeline_name ->
        process_name = Pipeline.Event.Producer.process_name(pipeline_name)

        assert match?(
                 %{
                   state: %{
                     paused: false
                   }
                 },
                 :sys.get_state(process_name)
               )

        assert Redis.Client.get!("flume_test:pipeline:#{pipeline_name}:paused") == "true"
      end)
    end

    test "resumes all the pipelines permanently" do
      assert Flume.pause_all(async: false, temporary: false) == [:ok]
      assert Flume.resume_all(async: false, temporary: false) == [:ok]

      Enum.each(Flume.Config.pipeline_names(), fn pipeline_name ->
        process_name = Pipeline.Event.Producer.process_name(pipeline_name)

        assert match?(
                 %{
                   state: %{
                     paused: false
                   }
                 },
                 :sys.get_state(process_name)
               )

        assert Redis.Client.get!("flume_test:pipeline:#{pipeline_name}:paused") == nil
      end)
    end
  end
end
