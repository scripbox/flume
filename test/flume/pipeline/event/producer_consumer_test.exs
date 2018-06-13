defmodule Flume.Pipeline.Event.ProducerConsumerTest do
  use TestWithRedis

  alias Flume.Pipeline.Event.ProducerConsumer
  alias Flume.Pipeline.Event, as: EventPipeline

  describe "handle_call/:new_events" do
    test "increments the pending events count" do
      pipeline_name = "pipeline_1"
      caller_name = :calling_process
      events_count = 10

      Process.register(self(), caller_name)

      {:ok, _} =
        TestProducer.start_link(%{process_name: "#{pipeline_name}_producer", queue: "test"})

      {:ok, producer_consumer} =
        ProducerConsumer.start_link(%{name: pipeline_name, max_demand: 10, interval: 5000})

      {:ok, pending_events, _, _} = EventPipeline.Stats.find(pipeline_name)
      assert pending_events == 0

      GenStage.call(producer_consumer, {:new_events, events_count})

      {:ok, pending_events, _, _} = EventPipeline.Stats.find(pipeline_name)
      assert pending_events == events_count
    end
  end
end
