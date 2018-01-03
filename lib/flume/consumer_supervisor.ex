defmodule Flume.ConsumerSupervisor do
  @moduledoc """
  producer/consumer and consumer stages are passed the
  pipeline name when they are started. This is essential
  for the producer/consumer to create a unique name that
  is also discoverable by the consumer in this pipeline.
  Both children also use the pipeline name for logging.
  """

  # Client API
  def start_link(state \\ %{}) do
    process_name = Enum.join([state.name, "supervisor"], "_")

    Supervisor.start_link(__MODULE__, state, name: String.to_atom(process_name))
  end

  # Server callbacks
  def init(state) do
    import Supervisor.Spec

    consumers = Enum.map(1..state.concurrency, fn(index) ->
      worker(Flume.Consumer, [state.name], id: index)
    end)

    children = [
      worker(Flume.ProducerConsumer, [producer_consumer_options(state)])
      | consumers
    ]

    opts = [strategy: :one_for_one,
            max_restarts: 20,
            max_seconds: 5,
            name: Flume.ConsumerSupervisor]
    supervise(children, opts)
  end

  # Private API
  defp producer_consumer_options(state) do
    max_demand = state.rate_limit_count || 1000
    interval = state.rate_limit_scale || 5000 # in milliseconds

    %{
      name: state.name,
      max_demand: max_demand,
      interval: interval
    }
  end
end
