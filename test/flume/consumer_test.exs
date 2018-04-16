defmodule Flume.ConsumerTest do
  use TestWithRedis

  alias Flume.Consumer
  alias Flume.Redis.Job

  @namespace Flume.Config.get(:namespace)

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

  describe "process event" do
    test "processes event if it is parseable" do
      # Start the worker process
      {:ok, _} = EchoWorker.start_link()

      pipeline_name = "pipeline_1"
      caller_name = :calling_process
      message = "hello world"

      Flume.PipelineStats.register(pipeline_name)
      Process.register(self(), caller_name)

      serialized_event = %{event_attributes() | args: [caller_name, message]} |> Poison.encode!()

      # Push the event to Redis
      Job.enqueue("#{@namespace}:queue:test", serialized_event)

      {:ok, producer} =
        TestProducer.start_link(%{
          process_name: "#{pipeline_name}_producer_consumer",
          queue: "test"
        })

      {:ok, _} = Consumer.start_link(%{name: pipeline_name})

      assert_receive {:received, ^message}

      # The consumer will also stop, since it is subscribed to the stage
      GenStage.stop(producer)
    end

    test "fails if event is not parseable" do
      # Start the worker process
      {:ok, _} = EchoWorker.start_link()

      pipeline_name = "pipeline_1"
      caller_name = :calling_process
      message = "hello world"

      Flume.PipelineStats.register(pipeline_name)
      Process.register(self(), caller_name)

      serialized_event = %{queue: "test", args: [caller_name, message]} |> Poison.encode!()

      # Push the event to Redis
      Job.enqueue("#{@namespace}:test", serialized_event)

      {:ok, producer} =
        TestProducer.start_link(%{
          process_name: "#{pipeline_name}_producer_consumer",
          queue: "test"
        })

      {:ok, _} = Consumer.start_link(%{name: pipeline_name})

      refute_receive {:received, ^message}

      # The consumer will also stop, since it is subscribed to the stage
      GenStage.stop(producer)
    end

    test "fails if bad/missing arguments are passed to the worker" do
      # Start the worker process
      {:ok, _} = EchoWorker.start_link()

      pipeline_name = "pipeline_1"
      caller_name = :calling_process
      message = "hello world"

      Flume.PipelineStats.register(pipeline_name)
      Process.register(self(), caller_name)

      serialized_event = %{event_attributes() | args: [caller_name]} |> Poison.encode!()

      # Push the event to Redis
      Job.enqueue("#{@namespace}:test", serialized_event)

      {:ok, producer} =
        TestProducer.start_link(%{
          process_name: "#{pipeline_name}_producer_consumer",
          queue: "test"
        })

      {:ok, _} = Consumer.start_link(%{name: pipeline_name})

      refute_receive {:received, ^message}

      # The consumer will also stop, since it is subscribed to the stage
      GenStage.stop(producer)
    end
  end
end
