defmodule Flume.Consumer do
  @moduledoc """
  Processes each event dispatched from the previous pipeline stage.
  This stage acts as a Consumer in the GenStage pipeline.

  Producer <- ProducerConsumer <- [**Consumer**]
  """
  use GenStage

  require Logger

  alias Flume.Job.Worker

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
    Logger.debug("#{state.name} [Consumer] received #{length(events)} events")

    # events will always be of size 1 as consumer has max_demand of 1
    [event | _] = events
    {:ok, pid} = Flume.DynamicSupervisor.start_child({Worker, []})
    Worker.execute(pid, event, state.name)
    Worker.stop(pid)

    {:noreply, [], state}
  end

  # Private API
  defp upstream_pipeline_name(pipeline_name) do
    Enum.join([pipeline_name, "producer_consumer"], "_")
    |> String.to_atom()
  end
end
