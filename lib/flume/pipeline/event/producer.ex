defmodule Flume.Pipeline.Event.Producer do
  @moduledoc """
  Polls for a batch of events from the source (Redis queue).
  This stage acts as a Producer in the GenStage pipeline.

  [**Producer**] <- ProducerConsumer <- Consumer
  """
  use GenStage

  require Flume.Logger

  alias Flume.Logger
  alias Flume.Pipeline.Event, as: EventPipeline

  # Client API
  def start_link(%{name: pipeline_name, queue: _queue} = state) do
    GenStage.start_link(__MODULE__, state, name: process_name(pipeline_name))
  end

  def stop(pipeline_name, timeout) do
    GenStage.call(process_name(pipeline_name), {:stop, timeout}, timeout)
  end

  # Server callbacks
  def init(state) do
    {:producer, state}
  end

  def handle_call({:stop, timeout}, _from, state) do
    :ok = EventPipeline.ProducerConsumer.stop(state.name, timeout)

    {:reply, :ok, [], state}
  end

  def handle_demand(demand, state) when demand > 0 do
    Logger.debug("#{state.name} [Producer] handling demand of #{demand}")
    {count, events} = take(demand, state.queue)

    Logger.debug("#{state.name} [Producer] pulled #{count} events from source")
    # synchronous call
    EventPipeline.Stats.update(:pending, state.name, count)

    {:noreply, events, state}
  end

  defp take(demand, queue_name) do
    events =
      case Flume.fetch_jobs(queue_name, demand) do
        {:error, error} ->
          Logger.error("#{queue_name} [Producer] error: #{error.reason}")
          []

        {:ok, events} ->
          events
      end

    count = length(events)

    {count, events}
  end

  def process_name(pipeline_name), do: :"#{pipeline_name}_producer"
end
