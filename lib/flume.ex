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
    if Config.get(:start_on_application) do
      start_link()
    else
      # Don't start Flume
      Supervisor.start_link([], strategy: :one_for_one)
    end
  end

  def start_link() do
    import Supervisor.Spec

    children = [
      worker(Redix, [Config.redis_opts(), Config.connection_opts()]),
      worker(Flume.Queue.Server, [Config.server_opts()]),
      worker(Flume.Queue.Scheduler, [Config.server_opts()]),
      worker(Flume.PipelineStatsSync, [])
      | Flume.Support.Pipelines.list
    ]

    opts = [
      strategy: :one_for_one,
      max_restarts: 20,
      max_seconds: 5,
      name: Flume.Supervisor
    ]
    Supervisor.start_link(children, opts)
  end
end
