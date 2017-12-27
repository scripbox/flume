defmodule Flume.Producer do
  use GenStage

  # Client API
  def start_link(%{name: pipeline_name, queue: _queue} = state) do
    process_name = Enum.join([pipeline_name, "producer"], "_")

    # initialize pending jobs to zero
    state = Map.put(state, :pending, 0)

    GenStage.start_link(__MODULE__, state, name: String.to_atom(process_name))
  end

  # Server callbacks
  def init(state) do
    {:producer, state}
  end

  def handle_demand(demand, state) when demand > 0 do
    # Add the current demand and the previously unsatisfied demand
    new_demand = demand + state.pending

    {count, events} = take(new_demand, state.pending, state.queue)

    # Update current state's pending jobs count
    state = Map.put(state, :pending, new_demand - count)
    {:noreply, events, state}
  end

  # Private API
  defp take(demand, counter, _queue_name) do
    # If the counter is 3 and we ask for 2 items, we will
    # emit the items 3 and 4, and set the state to 5.
    events = Enum.to_list(counter..counter+demand-1)
    count  = length(events)

    {count, events}
  end
end
