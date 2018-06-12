defmodule Flume.Event.Producer do
  @moduledoc """
  A producer will accept new events and these events will be
  dispatched to consumers.
  """

  use GenStage

  def start_link(initial) do
    GenStage.start_link(__MODULE__, initial, name: __MODULE__)
  end

  def init(initial) do
    {:producer, initial}
  end

  def enqueue(events) when is_list(events) do
    GenStage.cast(__MODULE__, {:enqueue, events})
  end

  def enqueue(event), do: enqueue([event])

  def handle_demand(demand, counter) when demand > 0 do
    {:noreply, [], counter}
  end

  def handle_cast({:enqueue, events}, counter) do
    events_count = length(events)

    {:noreply, events, counter + events_count}
  end
end
