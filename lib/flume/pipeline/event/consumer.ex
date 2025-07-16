defmodule Flume.Pipeline.Event.Consumer do
  @moduledoc """
  A Consumer will be a consumer supervisor that
  will spawn event processor for each event.
  """

  use ConsumerSupervisor

  require Flume.Instrumentation

  alias Flume.{Instrumentation}
  alias Flume.Pipeline.Event.Worker

  # Client API
  def start_link(state \\ %{}, worker \\ Worker) do
    ConsumerSupervisor.start_link(
      __MODULE__,
      {state, worker},
      name: process_name(state.name)
    )
  end

  # Server callbacks
  def init({state, worker}) do
    # Emit telemetry for consumer initialization
    Instrumentation.execute(
      [:flume, :consumer, :init],
      %{system_time: System.system_time()},
      %{
        pipeline_name: state.name,
        queue_name: state.queue,
        rate_limit_count: Map.get(state, :rate_limit_count),
        rate_limit_scale: Map.get(state, :rate_limit_scale),
        rate_limit_key: Map.get(state, :rate_limit_key),
        max_demand: state.max_demand
      },
      Map.get(state, :instrument, false)
    )

    instrument = Map.get(state, :instrument, false)

    children = [
      %{
        id: worker,
        start: {worker, :start_link, [%{name: state.name, instrument: instrument}]},
        restart: :temporary
      }
    ]

    upstream = upstream_process_name(state.name)

    {
      :ok,
      children,
      strategy: :one_for_one,
      max_restarts: 20,
      max_seconds: 10,
      subscribe_to: [{upstream, [min_demand: 0, max_demand: state.max_demand]}]
    }
  end

  def workers_count(process_name) do
    %{workers: workers_count} = ConsumerSupervisor.count_children(process_name)
    workers_count
  end

  defp process_name(pipeline_name) do
    :"#{pipeline_name}_consumer_supervisor"
  end

  defp upstream_process_name(pipeline_name) do
    :"#{pipeline_name}_producer_consumer"
  end
end
