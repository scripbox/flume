defmodule Flume.ConsumerSupervisor do
  @moduledoc """
  producer/consumer and consumer stages are passed the
  pipeline name when they are started. This is essential
  for the producer/consumer to create a unique name that
  is also discoverable by the consumer in this pipeline.
  Both children also use the pipeline name for logging.
  """
  use ConsumerSupervisor

  # Client API
  def start_link(%{name: pipeline_name, concurrency: _concurrency} = state) do
    process_name = Enum.join([pipeline_name, "supervisor"], "_")

    Supervisor.start_link(__MODULE__, state, name: String.to_atom(process_name))
  end

  # Server callbacks
  def init(state) do
    import Supervisor.Spec

    consumers = Enum.map(1..state.concurrency, fn(index) ->
      worker(Flume.Consumer, [state.name], id: index)
    end)

    children = [worker(Flume.ProducerConsumer, [%{name: state.name}]) | consumers]
    opts = [strategy: :one_for_one, name: "ConsumerSupervisor"]
    supervise(children, opts)
  end
end
