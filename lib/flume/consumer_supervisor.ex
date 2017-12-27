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
  def start_link(name) do
    Supervisor.start_link(__MODULE__, name, name: String.to_atom(name))
  end

  # Server callbacks
  def init(pipeline_name) do
    children = [
      worker(Flume.ProducerConsumer, [pipeline_name]),
      worker(Flume.Consumer, [pipeline_name])
    ]

    opts = [strategy: :one_for_one, name: "ConsumerSupervisor"]
    supervise(children, opts)
  end
end
