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

    GenStage.start_link(__MODULE__, state, name: String.to_atom(process_name))
  end

  # Server callbacks
  def init(state) do
    {:producer, state}
  end

  def handle_demand(demand, state) when demand > 0 do
    Logger.debug("#{state.name} [Producer] handling demand of #{demand}")
    {count, events} = take(demand, state.queue)

    Logger.debug("#{state.name} [Producer] pulled #{count} events from source")
    notify_new_events(state.name, count) # synchronous call
    {:noreply, events, state}
  end

  # Private API
  defp notify_new_events(pipeline_name, count) do
    downstream = Enum.join([pipeline_name, "producer_consumer"], "_") |> String.to_atom

    GenStage.call(downstream, {:new_events, count})
  end

  defp take(demand, queue_name) do
    events = case Flume.fetch_jobs(queue_name, demand) do
      [{:error, error}] ->
        Logger.error("#{queue_name} [Producer] error: #{error.reason}")
        []
      events -> events
    end
    count = length(events)

    {count, events}
  end
end
