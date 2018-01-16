defmodule FLume.ConsumerTest do
  use TestWithRedis

  alias Flume.Consumer
  alias Flume.Redis.Job

  @namespace Flume.Config.get(:namespace)

  def event_attributes do
    %{
      class: "EchoWorker",
      queue: "test",
      jid: "1082fd87-2508-4eb4-8fba-2958584a60e3",
      args: [],
      retry_count: 0,
      enqueued_at: 1514367662,
      finished_at: nil,
      failed_at: nil,
      retried_at: nil,
      error_message: nil,
      error_backtrace: nil
    }
  end

  describe "handle_events/3" do
    test "processes event if it is parseable" do
      {:ok, _} = EchoWorker.start_link() # Start the worker process

      pipeline_name = "pipeline_1"
      caller_name = :calling_process
      message = "hello world"

      Flume.PipelineStats.register(pipeline_name)
      Process.register(self(), caller_name)

      serialized_event = %{event_attributes() | args: [caller_name, message]} |> Poison.encode!

      # Push the event to Redis
      Job.enqueue(Flume.Redis, "#{@namespace}:test", serialized_event)

      {:ok, producer} = TestProducer.start_link(%{process_name: "#{pipeline_name}_producer_consumer", queue: "test"})
      {:ok, _} = Consumer.start_link(%{name: pipeline_name})

      assert_receive {:received, ^message}

      # The consumer will also stop, since it is subscribed to the stage
      GenStage.stop(producer)
    end

    test "fails if event is not parseable" do
      {:ok, _} = EchoWorker.start_link() # Start the worker process

      pipeline_name = "pipeline_1"
      caller_name = :calling_process
      message = "hello world"

      Flume.PipelineStats.register(pipeline_name)
      Process.register(self(), caller_name)

      serialized_event = %{queue: "test", args: [caller_name, message]} |> Poison.encode!

      # Push the event to Redis
      Job.enqueue(Flume.Redis, "#{@namespace}:test", serialized_event)

      {:ok, producer} = TestProducer.start_link(%{process_name: "#{pipeline_name}_producer_consumer", queue: "test"})
      {:ok, _} = Consumer.start_link(%{name: pipeline_name})

      refute_receive {:received, ^message}

      # The consumer will also stop, since it is subscribed to the stage
      GenStage.stop(producer)
    end

    test "fails if bad/missing arguments are passed to the worker" do
      {:ok, _} = EchoWorker.start_link() # Start the worker process

      pipeline_name = "pipeline_1"
      caller_name = :calling_process
      message = "hello world"

      Flume.PipelineStats.register(pipeline_name)
      Process.register(self(), caller_name)

      serialized_event = %{event_attributes() | args: [caller_name]} |> Poison.encode!

      # Push the event to Redis
      Job.enqueue(Flume.Redis, "#{@namespace}:test", serialized_event)

      {:ok, producer} = TestProducer.start_link(%{process_name: "#{pipeline_name}_producer_consumer", queue: "test"})
      {:ok, _} = Consumer.start_link(%{name: pipeline_name})

      refute_receive {:received, ^message}

      # The consumer will also stop, since it is subscribed to the stage
      GenStage.stop(producer)
    end
  end
end
