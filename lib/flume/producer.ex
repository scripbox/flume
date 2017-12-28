defmodule Flume.Producer do
  @moduledoc """
  Polls for a batch of events from the source (Redis queue).
  This stage acts as a Producer in the GenStage pipeline.

  [**Producer**] <- ProducerConsumer <- Consumer
  """
  use GenStage

  require Logger

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
    Logger.info("#{state.name} [Producer] handling demand of #{demand}, pending: #{state.pending}")
    {count, events} = take(demand, state.queue)

    Logger.info("#{state.name} [Producer] pulled #{count} events from source")
    {:noreply, events, state}
  end

  # Private API
  defp take(demand, _queue_name) do
    events = Enum.to_list(1..demand)
    count  = length(events)

    {count, events}
  end
end
