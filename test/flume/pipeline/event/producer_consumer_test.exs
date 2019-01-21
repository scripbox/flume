defmodule Flume.Pipeline.Event.ProducerConsumerTest do
  use TestWithRedis

  alias Flume.Pipeline
  alias Flume.Redis.Job
  alias Flume.Pipeline.Event.ProducerConsumer
  alias Flume.Pipeline.Event, as: EventPipeline

  @namespace Flume.Config.namespace()

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

  describe "handle_events/3" do
    test "groups similar events for bulk pipeline" do
      pipeline_name = "batch_pipeline"
      queue_name = "batch"
      caller_name = :calling_process

      EventPipeline.Stats.register(pipeline_name)
      Process.register(self(), caller_name)

      # Start the producer
      {:ok, producer} =
        TestProducer.start_link(%{
          process_name: "#{pipeline_name}_producer",
          queue: queue_name
        })

      # Push the event to Redis
      Enum.each(1..4, fn i ->
        Job.enqueue("#{@namespace}:queue:#{queue_name}", serialized_job("EchoWorker1", i))
      end)

      Enum.each(3..6, fn i ->
        Job.enqueue("#{@namespace}:queue:#{queue_name}", serialized_job("EchoWorker2", i))
      end)

      pipeline = %Pipeline{
        name: pipeline_name,
        max_demand: 10,
        batch_size: 2,
        interval: 10
      }

      # Start the producer_consumer
      {:ok, producer_consumer} = ProducerConsumer.start_link(pipeline)

      # Start the consumer
      {:ok, _} =
        EchoConsumer.start_link(
          producer_consumer,
          caller_name,
          name: :"#{pipeline_name}_consumer"
        )

      assert_receive {:received, [%Flume.BulkEvent{args: [[[1], [2]]], class: "EchoWorker1"}]}
      assert_receive {:received, [%Flume.BulkEvent{args: [[[3], [4]]], class: "EchoWorker1"}]}

      assert_receive {:received, [%Flume.BulkEvent{args: [[[3], [4]]], class: "EchoWorker2"}]}
      assert_receive {:received, [%Flume.BulkEvent{args: [[[5], [6]]], class: "EchoWorker2"}]}

      # The will stop the whole pipeline
      GenStage.stop(producer)
    end
  end

  describe "pause/1" do
    test "pauses the producer consumer from asking more events" do
      pipeline_name = "batch_pipeline"
      queue_name = "batch"
      caller_name = :calling_process

      EventPipeline.Stats.register(pipeline_name)
      Process.register(self(), caller_name)

      # Start the producer
      {:ok, producer} =
        TestProducer.start_link(%{
          process_name: "#{pipeline_name}_producer",
          queue: queue_name
        })

      pipeline = %Pipeline{
        name: pipeline_name,
        max_demand: 10,
        batch_size: 2,
        interval: 10
      }

      # Push the event to Redis
      Enum.each(1..4, fn i ->
        Job.enqueue("#{@namespace}:queue:#{queue_name}", serialized_job("EchoWorker1", i))
      end)

      # Start the producer_consumer
      {:ok, producer_consumer} = ProducerConsumer.start_link(pipeline)

      # Start the consumer
      {:ok, _} =
        EchoConsumer.start_link(
          producer_consumer,
          caller_name,
          name: :"#{pipeline_name}_consumer"
        )

      assert_receive {:received, [%Flume.BulkEvent{args: [[[1], [2]]], class: "EchoWorker1"}]}
      assert_receive {:received, [%Flume.BulkEvent{args: [[[3], [4]]], class: "EchoWorker1"}]}

      ProducerConsumer.pause(pipeline_name)

      Enum.each(3..6, fn i ->
        Job.enqueue("#{@namespace}:queue:#{queue_name}", serialized_job("EchoWorker2", i))
      end)

      refute_receive {:received, [%Flume.BulkEvent{args: [[[3], [4]]], class: "EchoWorker2"}]}
      refute_receive {:received, [%Flume.BulkEvent{args: [[[5], [6]]], class: "EchoWorker2"}]}

      # The will stop the whole pipeline
      GenStage.stop(producer)
    end
  end

  describe "resume/1" do
    test "resumes the producer consumer to ask for more events from the producer" do
      pipeline_name = "batch_pipeline"
      queue_name = "batch"
      caller_name = :calling_process

      EventPipeline.Stats.register(pipeline_name)
      Process.register(self(), caller_name)

      # Start the producer
      {:ok, producer} =
        TestProducer.start_link(%{
          process_name: "#{pipeline_name}_producer",
          queue: queue_name
        })

      pipeline = %Pipeline{
        name: pipeline_name,
        max_demand: 10,
        batch_size: 2,
        interval: 10
      }

      # Start the producer_consumer
      {:ok, producer_consumer} = ProducerConsumer.start_link(pipeline)

      # Start the consumer
      {:ok, _} =
        EchoConsumer.start_link(
          producer_consumer,
          caller_name,
          name: :"#{pipeline_name}_consumer"
        )

      :ok = ProducerConsumer.pause(pipeline_name)

      Enum.each(1..4, fn i ->
        Job.enqueue("#{@namespace}:queue:#{queue_name}", serialized_job("EchoWorker1", i))
      end)

      Enum.each(3..6, fn i ->
        Job.enqueue("#{@namespace}:queue:#{queue_name}", serialized_job("EchoWorker2", i))
      end)

      refute_receive {:received, [%Flume.BulkEvent{args: [[[1], [2]]], class: "EchoWorker1"}]}
      refute_receive {:received, [%Flume.BulkEvent{args: [[[3], [4]]], class: "EchoWorker1"}]}
      refute_receive {:received, [%Flume.BulkEvent{args: [[[3], [4]]], class: "EchoWorker2"}]}
      refute_receive {:received, [%Flume.BulkEvent{args: [[[3], [4]]], class: "EchoWorker2"}]}

      ProducerConsumer.resume(pipeline_name)

      assert_receive {:received, [%Flume.BulkEvent{args: [[[1], [2]]], class: "EchoWorker1"}]}
      assert_receive {:received, [%Flume.BulkEvent{args: [[[3], [4]]], class: "EchoWorker1"}]}

      assert_receive {:received, [%Flume.BulkEvent{args: [[[3], [4]]], class: "EchoWorker2"}]}
      assert_receive {:received, [%Flume.BulkEvent{args: [[[5], [6]]], class: "EchoWorker2"}]}

      # The will stop the whole pipeline
      GenStage.stop(producer)
    end
  end
end
