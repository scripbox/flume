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
      worker(Flume.Consumer, [%{name: state.name}], id: index)
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
    max_demand = case Integer.parse(to_string(state.rate_limit_count)) do
      {count, _} -> count
      :error -> 1000 # default max demand
    end

    interval = case Integer.parse(to_string(state.rate_limit_scale)) do
      {scale, _} -> scale
      :error -> 5000 # in milliseconds
    end

    %{
      name: state.name,
      max_demand: max_demand,
      interval: interval
    }
  end
end
