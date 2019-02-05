defmodule Flume.Pipeline.Event.ProducerConsumer do
  @moduledoc """
  Takes a batch of events periodically to be sent to the consumers.
  This stage acts as a Producer-Consumer in the GenStage pipeline.

  Producer <- [**ProducerConsumer**] <- Consumer
  """
  use GenStage

  require Flume.Logger

  alias Flume.{BulkEvent, Event, Logger}

  # Client API
  def start_link(%{} = pipeline) do
    GenStage.start_link(__MODULE__, pipeline, name: process_name(pipeline.name))
  end

  # Server callbacks
  def init(state) do
    upstream = upstream_process_name(state.name)

    {:producer_consumer, state,
     subscribe_to: [{upstream, min_demand: 0, max_demand: state.max_demand}]}
  end

  # Process events one-by-one when batch_size is not set
  def handle_events(events, _from, %{batch_size: nil} = state) do
    Logger.debug("#{state.name} [ProducerConsumer] received #{length(events)} events")

    {:noreply, events, state}
  end

  # Group the events by the specified :batch_size
  # The consumer will receive each group as a single event
  # and process the group together
  def handle_events(events, _from, state) do
    Logger.debug("#{state.name} [ProducerConsumer] received #{length(events)} events")

    grouped_events = group_similar_events(events, state.batch_size)

    {:noreply, grouped_events, state}
  end

  defp process_name(pipeline_name) do
    :"#{pipeline_name}_producer_consumer"
  end

  defp upstream_process_name(pipeline_name), do: :"#{pipeline_name}_producer"

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
