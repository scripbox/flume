defmodule Flume.Pipeline.Event.ProducerConsumer do
  @moduledoc """
  Takes a batch of events periodically to be sent to the consumers.
  This stage acts as a Producer-Consumer in the GenStage pipeline.

  Producer <- [**ProducerConsumer**] <- Consumer
  """
  use GenStage

  require Logger
  alias Flume.Pipeline.Event, as: EventPipeline

  # Client API
  def start_link(state \\ %{}) do
    GenStage.start_link(__MODULE__, state, name: process_name(state.name))
  end

  # Server callbacks
  def init(state) do
    upstream = upstream_process_name(state.name)

    # Register the pipeline in :ets
    EventPipeline.Stats.register(state.name)

    {:producer_consumer, state,
     subscribe_to: [{upstream, min_demand: 0, max_demand: state.max_demand}]}
  end

  def handle_subscribe(:producer, _opts, from, state) do
    # Register the producer in the state
    state = Map.put(state, :producer, from)

    # Ask for the pending events and schedule the next time around
    state = ask_and_schedule(state, from)

    # Returns manual as we want control over the demand
    {:manual, state}
  end

  def handle_subscribe(:consumer, _opts, _from, state) do
    {:automatic, state}
  end

  def handle_events(events, _from, state) do
    Logger.debug("#{state.name} [ProducerConsumer] received #{length(events)} events")

    {:noreply, events, state}
  end

  # The producer notifies when it delivers new events
  def handle_cast({:new_events, count}, state) do
    EventPipeline.Stats.update(:pending, state.name, count)

    {:noreply, [], state}
  end

  def handle_info({:ask, from}, state) do
    # This callback is invoked by the Process.send_after/3 message below.
    {:noreply, [], ask_and_schedule(state, from)}
  end

  # Private API
  defp ask_and_schedule(state, from) do
    {:ok, pending_events, _, _} = EventPipeline.Stats.find(state.name)

    events_to_ask =
      cond do
        pending_events == 0 ->
          Logger.debug(
            "#{state.name} [ProducerConsumer] [No Events] consider asking #{state.max_demand} events"
          )

          state.max_demand

        pending_events == state.max_demand ->
          Logger.debug(
            "#{state.name} [ProducerConsumer] [Max Pending Events] consider asking 0 events"
          )

          0

        pending_events < state.max_demand ->
          new_demand = state.max_demand - pending_events

          Logger.debug(
            "#{state.name} [ProducerConsumer] [Finished Events less than MAX] asking #{new_demand} events"
          )

          new_demand

        true ->
          0
      end

    if events_to_ask > 0 do
      Logger.debug("#{state.name} [ProducerConsumer] asking #{events_to_ask} events")

      GenStage.ask(from, events_to_ask)
    end

    # Schedule the next request
    Process.send_after(self(), {:ask, from}, state.interval)
    state
  end

  defp process_name(pipeline_name) do
    :"#{pipeline_name}_producer_consumer"
  end

  defp upstream_process_name(pipeline_name) do
    :"#{pipeline_name}_producer"
  end
end
