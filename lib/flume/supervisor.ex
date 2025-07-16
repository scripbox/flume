defmodule Flume.Supervisor do
  @moduledoc """
  Flume is a job processing system backed by Redis & GenStage.
  Each pipeline processes jobs from a specific Redis queue.
  Flume has a retry mechanism that keeps retrying the jobs with an exponential backoff.

  ## Configuration Options

  The supervisor accepts configuration options at startup, allowing multiple instances
  with different settings:

  ```elixir
  config = %{
    name: :my_flume,
    namespace: "my_app",
    pipelines: [
      %{name: "high", queue: "high_priority", max_demand: 100},
      %{name: "low", queue: "low_priority", max_demand: 10}
    ],
    redis_pool_size: 5,
    host: "localhost",
    port: 6379
  }

  {:ok, pid} = Flume.Supervisor.start_link(config)
  ```
  """
  use Application

  alias Flume.Config

  def start(_type, _args) do
    Supervisor.start_link([], strategy: :one_for_one)
  end

  def start_link(config \\ %{}) do
    children = build_children(config)

    opts = [
      strategy: :rest_for_one,
      max_restarts: 20,
      max_seconds: 5,
      name: supervisor_name(config)
    ]

    {:ok, _pid} = Supervisor.start_link(children, opts)
  end

  defp build_children(config) do
    # Always start Config GenServer first
    base_children = [
      {Flume.Config, config}
    ]

    if Map.get(config, :mock, Config.get(:mock, false)) do
      base_children
    else
      # This order matters, first we need to start config and redis workers
      # then all other processes.
      base_children ++
        [
          Flume.Redis.Supervisor,
          {Flume.Queue.Scheduler, scheduler_opts_from_config(config)},
          Flume.Pipeline.SystemEvent.Supervisor,
          {Task.Supervisor, [name: task_supervisor_name(config)]}
        ] ++ pipeline_children_from_config(config)
    end
  end

  defp supervisor_name(config) do
    case Map.get(config, :name) do
      nil -> Flume.Supervisor
      name -> :"#{name}.Supervisor"
    end
  end

  defp task_supervisor_name(config) do
    case Map.get(config, :name) do
      nil -> Flume.SafeApplySupervisor
      name -> :"#{name}.SafeApplySupervisor"
    end
  end

  defp scheduler_opts_from_config(config) do
    [
      namespace: Map.get(config, :namespace, Config.get(:namespace, "flume")),
      scheduler_poll_interval: Map.get(config, :scheduler_poll_interval, Config.get(:scheduler_poll_interval, 10_000))
    ]
  end

  defp pipeline_children_from_config(config) do
    pipelines = Map.get(config, :pipelines, Config.get(:pipelines, []))

    Enum.flat_map(pipelines, fn pipeline ->
      pipeline_struct = Flume.Pipeline.new(pipeline)
      scheduler_options = scheduler_opts_from_config(config) ++ [queue: pipeline.queue]
      Flume.Pipeline.Event.attach_instrumentation(pipeline_struct)

      [
        %{
          id: {Flume.Pipeline.Event.Producer, generate_id()},
          start: {Flume.Pipeline.Event.Producer, :start_link, [pipeline_struct]}
        },
        %{
          id: {Flume.Pipeline.Event.ProducerConsumer, generate_id()},
          start: {Flume.Pipeline.Event.ProducerConsumer, :start_link, [pipeline_struct]}
        },
        %{
          id: {Flume.Pipeline.Event.Consumer, generate_id()},
          start: {Flume.Pipeline.Event.Consumer, :start_link, [pipeline_struct]}
        },
        %{
          id: {Flume.Queue.ProcessingScheduler, generate_id()},
          start: {Flume.Queue.ProcessingScheduler, :start_link, [scheduler_options]}
        }
      ]
    end)
  end

  defp generate_id do
    <<part1::32, part2::32>> = :crypto.strong_rand_bytes(8)
    "#{part1}#{part2}"
  end
end
