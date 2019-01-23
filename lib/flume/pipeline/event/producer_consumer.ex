defmodule Flume.Pipeline.Event.ProducerConsumer do
  @moduledoc """
  Takes a batch of events periodically to be sent to the consumers.
  This stage acts as a Producer-Consumer in the GenStage pipeline.

  Producer <- [**ProducerConsumer**] <- Consumer
  """
  use GenStage

  require Flume.Logger

  alias Flume.{BulkEvent, Event, Logger}
  alias Flume.Pipeline.Event, as: EventPipeline

  # Client API
  def start_link(%{} = pipeline) do
    GenStage.start_link(__MODULE__, pipeline, name: process_name(pipeline.name))
  end

  def pause(pipeline_name) do
    GenStage.cast(process_name(pipeline_name), :pause)
  end

  def resume(pipeline_name) do
    GenStage.cast(process_name(pipeline_name), :resume)
  end

  # Server callbacks
  def init(pipeline) do
    {state, upstream} = EventPipeline.init(pipeline)

    {:producer_consumer, state,
     subscribe_to: [{upstream, min_demand: 0, max_demand: state.max_demand}]}
  end

  def handle_cast(:pause, %{paused: true} = state) do
    {:noreply, [], state}
  end

  def handle_cast(:pause, state) do
    state = Map.put(state, :paused, true)

    {:noreply, [], state}
  end

  def handle_cast(:resume, %{paused: true} = state) do
    state = Map.delete(state, :paused)

    {:noreply, [], state}
  end

  def handle_cast(:resume, state) do
    {:noreply, [], state}
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

  # Process events one-by-one when batch_size is not set
  def handle_events(events, _from, %{batch_size: nil} = state) do
    Logger.debug("#{state.name} [ProducerConsumer] received #{length(events)} events")

    {:noreply, events, state}
  end

  # Group the events in groups of the specified :batch_size
  # The consumer will receive each group as a single event
  # and process the group in one batch.
  def handle_events(events, _from, state) do
    Logger.debug("#{state.name} [ProducerConsumer] received #{length(events)} events")

    grouped_events = group_similar_events(events, state.batch_size)

    {:noreply, grouped_events, state}
  end

  def handle_info({:ask, from}, state) do
    # This callback is invoked by the Process.send_after/3 message below.
    {:noreply, [], ask_and_schedule(state, from)}
  end

  defp ask_and_schedule(%{paused: true} = state, from) do
    Process.send_after(self(), {:ask, from}, state.interval)
    state
  end

  # Private API
  defp ask_and_schedule(state, from) do
    {:ok, pending_events, _, _} = EventPipeline.Stats.find(state.name)

    max_demand =
      if state.batch_size == nil do
        state.max_demand
      else
        # validate batch_size to be an integer
        state.max_demand * state.batch_size
      end

    events_to_ask =
      cond do
        pending_events == 0 ->
          Logger.debug(
            "#{state.name} [ProducerConsumer] [No Events] consider asking #{max_demand} events"
          )

          max_demand

        pending_events == max_demand ->
          Logger.debug(
            "#{state.name} [ProducerConsumer] [Max Pending Events] consider asking 0 events"
          )

          0

        pending_events < max_demand ->
          new_demand = max_demand - pending_events

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

  defp group_similar_events(events, batch_size) do
    events
    |> Enum.map(&Event.decode!/1)
    |> Enum.group_by(& &1.class)
    |> Map.values()
    |> Enum.flat_map(fn event_group ->
      event_group
      |> Enum.chunk_every(batch_size)
      |> Enum.map(&BulkEvent.new/1)
    end)
  end
end
