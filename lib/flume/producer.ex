defmodule Flume.Producer do
  use GenStage

  # Client API
  def start_link do
    GenStage.start_link(__MODULE__, 0, name: __MODULE__)
  end

  # Server callbacks
  def init(count) do
    {:producer, count}
  end

  def handle_demand(demand, state) when demand > 0 do
    # Add the current demand and the previously unsatisfied demand
    new_demand = demand + state

    {count, events} = take(new_demand, state)

    {:noreply, events, new_demand - count}
  end

  # Private API
  defp take(demand, counter) do
    # If the counter is 3 and we ask for 2 items, we will
    # emit the items 3 and 4, and set the state to 5.
    events = Enum.to_list(counter..counter+demand-1)
    count  = length(events)

    {count, events}
  end
end
