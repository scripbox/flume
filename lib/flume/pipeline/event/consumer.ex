defmodule Flume.Pipeline.Event.Consumer do
  @moduledoc """
  A Consumer will be a consumer supervisor that
  will spawn event processor for each event.
  """

  use ConsumerSupervisor

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
    instrument = Map.get(state, :instrument, false)

    children = [
      worker(worker, [%{name: state.name, instrument: instrument}], restart: :temporary)
    ]

    upstream = upstream_process_name(state.name)

    {
      :ok,
      children,
      strategy: :one_for_one,
      max_restarts: 20,
      max_seconds: 10,
      subscribe_to: [{upstream, [state]}]
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
