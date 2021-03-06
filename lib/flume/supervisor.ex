defmodule Flume.Supervisor do
  @moduledoc """
  Flume is a job processing system backed by Redis & GenStage.
  Each pipeline processes jobs from a specific Redis queue.
  Flume has a retry mechanism that keeps retrying the jobs with an exponential backoff.
  """
  use Application

  import Supervisor.Spec

  alias Flume.Config

  def start(_type, _args) do
    Supervisor.start_link([], strategy: :one_for_one)
  end

  def start_link do
    children =
      if Config.mock() do
        []
      else
        # This order matters, first we need to start all redix worker processes
        # then all other processes.
        [
          supervisor(Flume.Redis.Supervisor, []),
          worker(Flume.Queue.Scheduler, [Config.scheduler_opts()]),
          supervisor(Flume.Pipeline.SystemEvent.Supervisor, []),
          supervisor(Task.Supervisor, [[name: Flume.SafeApplySupervisor]])
        ] ++ Flume.Support.Pipelines.list()
      end

    opts = [
      strategy: :one_for_one,
      max_restarts: 20,
      max_seconds: 5,
      name: Flume.Supervisor
    ]

    {:ok, _pid} = Supervisor.start_link(children, opts)
  end
end
