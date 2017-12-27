defmodule Flume.Consumer do
  use GenStage

  # Client API
  def start_link do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  # Server Callbacks
  def init(:ok) do
    {:consumer, :state_does_not_matter, subscribe_to: [{Flume.ProducerConsumer, min_demand: 0, max_demand: 1}]}
  end

  def handle_events(events, _from, state) do
    # Inspect the events.
    IO.inspect(events)

    # We are a consumer, so we would never emit items.
    {:noreply, [], state}
  end
end
