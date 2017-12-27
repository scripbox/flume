defmodule Flume.ProducerConsumer do
  use GenStage

  require Logger

  # Client API
  def start_link(%{name: pipeline_name} = state) do
    process_name = Enum.join([pipeline_name, "producer_consumer"], "_")

    GenStage.start_link(__MODULE__, state, name: String.to_atom(process_name))
  end

  # Server callbacks
  def init(state) do
    upstream = Enum.join([state.name, "producer"], "_")

    {:producer_consumer, state, subscribe_to: [{String.to_atom(upstream), min_demand: 0, max_demand: 1}]}
  end

  def handle_events(events, _from, state) do
    Logger.info("#{state.name} ProducerConsumer received #{length events} events")
    Logger.info(events)

    {:noreply, events, state}
  end
end
