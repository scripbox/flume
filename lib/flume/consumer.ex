defmodule Flume.Consumer do
  @moduledoc """
  Processes each event dispatched from the previous pipeline stage.
  This stage acts as a Consumer in the GenStage pipeline.

  Producer <- ProducerConsumer <- [**Consumer**]
  """
  use GenStage

  require Logger

  # Client API
  def start_link(state \\ %{}) do
    GenStage.start_link(__MODULE__, state)
  end

  # Server Callbacks
  def init(state) do
    upstream = upstream_pipeline_name(state.name)
    {:consumer, state, subscribe_to: [{upstream, min_demand: 0, max_demand: 1}]}
  end

  def handle_events(events, _from, state) do
    Logger.info("#{state.name} [Consumer] received #{length events} events")
    # process events here...
    Logger.info("#{state.name} [Consumer] finished #{length events} events")

    notify_done(state.name) # synchronous call

    {:noreply, [], state}
  end

  # Private API
  defp notify_done(pipeline_name) do
    upstream = upstream_pipeline_name(pipeline_name)

    GenStage.call(upstream, {:consumer_done, self()})
  end

  defp upstream_pipeline_name(pipeline_name) do
    Enum.join([pipeline_name, "producer_consumer"], "_")
    |> String.to_atom
  end
end
