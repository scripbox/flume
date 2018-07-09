defmodule Flume.Pipeline.SystemEvent.Supervisor do
  use Supervisor

  alias Flume.Pipeline.SystemEvent

  def start_link do
    children = [
      worker(SystemEvent.Producer, [0]),
      worker(SystemEvent.Consumer, [])
    ]

    opts = [
      strategy: :one_for_one,
      max_restarts: 20,
      max_seconds: 10,
      name: __MODULE__
    ]

    Supervisor.start_link(children, opts)
  end

  def init(opts) do
    {:ok, opts}
  end
end
