defmodule Flume.Pipeline.Event.ConsumerTest do
  use TestWithRedis

  alias Flume.Pipeline
  alias Flume.Redis.Job
  alias Flume.Pipeline.Event.Consumer

  @namespace Flume.Config.namespace()

  def event_attributes do
    %{
      class: "EchoWorker",
      function: "perform",
      queue: "test",
      jid: "1082fd87-2508-4eb4-8fba-2958584a60e3",
      args: [],
      retry_count: 0,
      enqueued_at: 1_514_367_662,
      finished_at: nil,
      failed_at: nil,
      retried_at: nil,
      error_message: nil,
      error_backtrace: nil
    }
  end

  describe "handle_events/3" do
    test "processes event if it is parseable" do
      # Start the worker process
      {:ok, _} = EchoWorker.start_link()

      pipeline_name = "pipeline_1"
      queue_name = "test"
      caller_name = :calling_process
      message = "hello world"

      pipeline = %Pipeline{
        name: pipeline_name,
        queue: queue_name,
        max_demand: 10,
        batch_size: 2
      }

      Process.register(self(), caller_name)

      serialized_event = TestWithRedis.serialized_job("EchoWorker", [caller_name, message])

      # Push the event to Redis
      Job.enqueue("#{@namespace}:queue:test", serialized_event)

      {:ok, producer} =
        TestProducer.start_link(%{
          process_name: "#{pipeline_name}_producer_consumer",
          queue: "test"
        })

      {:ok, _} = Consumer.start_link(pipeline)

      assert_receive {:received, ^message}

      # The consumer will also stop, since it is subscribed to the stage
      GenStage.stop(producer)
    end

    test "fails if event is not parseable" do
      # Start the worker process
      {:ok, _} = EchoWorker.start_link()

      pipeline_name = "pipeline_1"
      queue_name = "test"
      caller_name = :calling_process
      message = "hello world"

      pipeline = %Pipeline{
        name: pipeline_name,
        queue: queue_name,
        max_demand: 10,
        batch_size: 2
      }

      Process.register(self(), caller_name)

      serialized_event = %{queue: "test", args: [caller_name, message]} |> Jason.encode!()

      # Push the event to Redis
      Job.enqueue("#{@namespace}:test", serialized_event)

      {:ok, producer} =
        TestProducer.start_link(%{
          process_name: "#{pipeline_name}_producer_consumer",
          queue: "test"
        })

      {:ok, _} = Consumer.start_link(pipeline)

      refute_receive {:received, ^message}

      # The consumer will also stop, since it is subscribed to the stage
      GenStage.stop(producer)
    end

    test "fails if bad/missing arguments are passed to the worker" do
      # Start the worker process
      {:ok, _} = EchoWorker.start_link()

      pipeline_name = "pipeline_1"
      queue_name = "test"
      caller_name = :calling_process
      message = "hello world"

      pipeline = %Pipeline{
        name: pipeline_name,
        queue: queue_name,
        max_demand: 10,
        batch_size: 2
      }

      Process.register(self(), caller_name)

      serialized_event = %{event_attributes() | args: [caller_name]} |> Jason.encode!()

      # Push the event to Redis
      Job.enqueue("#{@namespace}:test", serialized_event)

      {:ok, producer} =
        TestProducer.start_link(%{
          process_name: "#{pipeline_name}_producer_consumer",
          queue: "test"
        })

      {:ok, _} = Consumer.start_link(pipeline)

      refute_receive {:received, ^message}

      # The consumer will also stop, since it is subscribed to the stage
      GenStage.stop(producer)
    end
  end
end
