defmodule Flume.ProducerConsumer do
  use GenStage

  # Client API
  def start_link(pipeline_name) do
    process_name = Enum.join([pipeline_name, "ProducerConsumer"], "")
    GenStage.start_link(__MODULE__, pipeline_name, name: String.to_atom(process_name))
  end

  # Server callbacks
  def init(pipeline_name) do
    {:producer_consumer, pipeline_name, subscribe_to: [{Flume.Producer, min_demand: 5, max_demand: 10}]}
  end

  def handle_events(events, _from, pipeline_name) do
    IO.puts "#{pipeline_name} ProducerConsumer received #{length events} events"
    IO.inspect(events)

    {:noreply, events, pipeline_name}
  end
end
