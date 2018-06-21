defmodule Flume.Pipeline.Event.Producer do
  @moduledoc """
  Polls for a batch of events from the source (Redis queue).
  This stage acts as a Producer in the GenStage pipeline.

  [**Producer**] <- ProducerConsumer <- Consumer
  """
  use GenStage

  require Logger

  # Client API
  def start_link(%{name: pipeline_name, queue: _queue} = state) do
    process_name = :"#{pipeline_name}_producer"

    GenStage.start_link(__MODULE__, state, name: process_name)
  end

  # Server callbacks
  def init(state) do
    {:producer, state}
  end

  def handle_demand(demand, state) when demand > 0 do
    Logger.debug("#{state.name} [Producer] handling demand of #{demand}")
    {count, events} = take(demand, state.queue)

    Logger.debug("#{state.name} [Producer] pulled #{count} events from source")

    {:noreply, events, state}
  end

  defp take(demand, queue_name) do
    events =
      case Flume.fetch_jobs(queue_name, demand) do
        [{:error, error}] ->
          Logger.error("#{queue_name} [Producer] error: #{error.reason}")
          []

        events ->
          events
      end

    count = length(events)

    {count, events}
  end
end
