defmodule Flume.Pipeline.Event.Consumer do
  @moduledoc """
  A Consumer will be a consumer supervisor that
  will spawn event processor for each event.
  """

  use ConsumerSupervisor

  alias Flume.Pipeline.Event.Worker

  # Client API
  def start_link(state \\ %{}) do
    process_name = :"#{state.name}_consumer_supervisor"

    ConsumerSupervisor.start_link(__MODULE__, state, name: process_name)
  end

  # Server callbacks
  def init(state) do
    children = [
      worker(Worker, [%{name: state.name}], restart: :temporary)
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

  # Private API

  defp upstream_process_name(pipeline_name) do
    :"#{pipeline_name}_producer_consumer"
  end
end
