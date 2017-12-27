defmodule Flume do
  @moduledoc """
  Flume is a job processing system backed by Redis & GenStage.
  Each pipeline processes jobs from a specific Redis queue.
  Flume has a retry mechanism that keeps retrying the jobs with an exponential backoff.
  """
  use Application
  use Flume.Queue

  alias Flume.Config

  def start(_type, _args) do
    import Supervisor.Spec

    children = [
      worker(Redix, [Config.redis_opts(), Config.connection_opts()]),
      worker(Flume.Queue.Server, [Config.server_opts()])
      | Flume.Support.Pipelines.list
    ]

    opts = [strategy: :one_for_one, name: Flume.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
