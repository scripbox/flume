defmodule Flume do
  @moduledoc """
  Flume is a job processing system backed by Redis & GenStage.
  Each pipeline processes jobs from a specific Redis queue.
  Flume has a retry mechanism that keeps retrying the jobs with an exponential backoff.
  """
  use Application
  use Flume.API

  import Supervisor.Spec

  alias Flume.Config

  def start(_type, _args) do
    if Config.start_on_application() do
      start_link()
    else
      # Don't start Flume
      Supervisor.start_link([], strategy: :one_for_one)
    end
  end

  def start_link() do
    children = [
      supervisor(Flume.Redis.Supervisor, []),
      worker(Flume.Queue.Scheduler, [Config.scheduler_opts()]),
      supervisor(Flume.Pipeline.SystemEvent.Supervisor, []),
      supervisor(Task.Supervisor, [[name: Flume.SafeApplySupervisor]])
    ]

    # This order matters, first we need to start all redix worker processes
    # then all other processes.
    children = children ++ Flume.Support.Pipelines.list()

    opts = [
      strategy: :one_for_one,
      max_restarts: 20,
      max_seconds: 5,
      name: Flume.Supervisor
    ]

    {:ok, _pid} = Supervisor.start_link(children, opts)
  end
end
