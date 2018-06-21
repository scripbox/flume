defmodule Flume do
  @moduledoc """
  Flume is a job processing system backed by Redis & GenStage.
  Each pipeline processes jobs from a specific Redis queue.
  Flume has a retry mechanism that keeps retrying the jobs with an exponential backoff.
  """
  use Application
  use Flume.Queue

  import Supervisor.Spec

  alias Flume.Config

  @queue_server_pool_name :"Flume.Queue.Server.pool"
  @redix_worker_prefix "flume_redix"

  def start(_type, _args) do
    if Config.get(:start_on_application) do
      start_link()
    else
      # Don't start Flume
      Supervisor.start_link([], strategy: :one_for_one)
    end
  end

  def start_link() do
    children = [
      queue_server_pool_spec(Config.server_opts()),
      worker(Flume.Queue.Scheduler, [Config.server_opts()]),
      supervisor(Flume.Pipeline.SystemEvent.Supervisor, [])
    ]

    # This order matters, first we need to start all redix worker processes
    # then all other processes.
    children = redix_worker_spec() ++ children ++ Flume.Support.Pipelines.list()

    opts = [
      strategy: :one_for_one,
      max_restarts: 20,
      max_seconds: 5,
      name: Flume.Supervisor
    ]

    Supervisor.start_link(children, opts)
  end

  def queue_server_pool_name do
    @queue_server_pool_name
  end

  def redix_worker_prefix do
    @redix_worker_prefix
  end

  # Private API

  defp queue_server_pool_spec(args) do
    pool_name = queue_server_pool_name()

    args = [
      [
        name: {:local, pool_name},
        worker_module: Flume.Queue.Server,
        size: Config.get(:server_pool_size),
        max_overflow: 0
      ],
      args
    ]

    shutdown_timeout = Config.get(:server_shutdown_timeout)

    worker(:poolboy, args, restart: :permanent, shutdown: shutdown_timeout, id: pool_name)
  end

  defp redix_worker_spec() do
    pool_size = Config.redis_pool_size()

    # Create the redix children list of workers:
    for i <- 0..(pool_size - 1) do
      connection_opts =
        Keyword.put(Config.connection_opts(), :name, :"#{redix_worker_prefix()}_#{i}")

      args = [Config.redis_opts(), connection_opts]
      worker(Redix, args, id: {Redix, i})
    end
  end
end
