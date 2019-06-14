defmodule Flume.Pipeline.Event.ProducerConsumerTest do
  use TestWithRedis

  alias Flume.Pipeline
  alias Flume.Redis.Job
  alias Flume.Pipeline.Event.ProducerConsumer

  @namespace Flume.Config.namespace()

  describe "handle_events/3" do
    test "groups similar events for bulk pipeline" do
      pipeline_name = "batch_pipeline"
      queue_name = "batch"
      caller_name = :calling_process

      Process.register(self(), caller_name)

      # Start the producer
      {:ok, producer} =
        TestProducer.start_link(%{
          process_name: "#{pipeline_name}_producer",
          queue: queue_name
        })

      # Push the event to Redis
      Enum.each(1..4, fn i ->
        Job.enqueue(
          "#{@namespace}:queue:#{queue_name}",
          TestWithRedis.serialized_job("EchoWorker1", [i])
        )
      end)

      Enum.each(3..6, fn i ->
        Job.enqueue(
          "#{@namespace}:queue:#{queue_name}",
          TestWithRedis.serialized_job("EchoWorker2", [i])
        )
      end)

      pipeline = %Pipeline{
        name: pipeline_name,
        max_demand: 10,
        batch_size: 2
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
end
