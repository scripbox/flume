defmodule Flume.Consumer do
  @moduledoc """
  producer/consumer and consumer stages are passed the
  pipeline name when they are started. This is essential
  for the producer/consumer to create a unique name that
  is also discoverable by the consumer in this pipeline.
  Both children also use the pipeline name for logging.
  """

  use ConsumerSupervisor

  # Client API
  def start_link(state \\ %{}) do
    process_name = :"#{state.name}_consumer_supervisor"

    ConsumerSupervisor.start_link(__MODULE__, state, name: process_name)
  end

  # Server callbacks
  def init(state) do
    children = [
      worker(Flume.EventProcessor, [%{name: state.name}], restart: :temporary)
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
