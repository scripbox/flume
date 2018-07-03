defmodule EchoConsumer do
  use GenStage

  def start_link(producer, owner, name: process_name) do
    GenStage.start_link(__MODULE__, {producer, owner}, name: process_name)
  end

  def init({producer, owner}) do
    {:consumer, owner, subscribe_to: [{producer, min_demand: 0, max_demand: 1}]}
  end

  def handle_events(events, _, owner) do
    send(owner, {:received, events})
    {:noreply, [], owner}
  end

  # The producer notifies when it delivers new events
  def handle_cast({:new_events, _count}, state) do
    {:noreply, [], state}
  end
end
