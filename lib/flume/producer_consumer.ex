defmodule Flume.ProducerConsumer do
  use GenStage

  # Client API
  def start_link do
    GenStage.start_link(__MODULE__, 0, name: __MODULE__)
  end

  # Server callbacks
  def init(number) do
    {:producer_consumer, number, subscribe_to: [{Flume.Producer, min_demand: 0, max_demand: 1}]}
  end

  def handle_events(events, _from, number) do
    {:noreply, events, number}
  end
end
