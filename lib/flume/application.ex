defmodule Flume.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    # Get configuration from application environment
    config = %{
      name: Application.get_env(:flume, :name, Flume),
      host: Application.get_env(:flume, :host, "127.0.0.1"),
      port: Application.get_env(:flume, :port, 6379),
      namespace: Application.get_env(:flume, :namespace, "flume"),
      database: Application.get_env(:flume, :database, 0),
      redis_timeout: Application.get_env(:flume, :redis_timeout, 5000),
      redis_pool_size: Application.get_env(:flume, :redis_pool_size, 10),
      pipelines: Application.get_env(:flume, :pipelines, []),
      backoff_initial: Application.get_env(:flume, :backoff_initial, 500),
      backoff_max: Application.get_env(:flume, :backoff_max, 10_000),
      scheduler_poll_interval: Application.get_env(:flume, :scheduler_poll_interval, 10_000),
      max_retries: Application.get_env(:flume, :max_retries, 10),
      instrumentation: Application.get_env(:flume, :instrumentation, [
        handler_module: Flume.Instrumentation.DefaultEventHandler,
        handler_function: :handle,
        metadata: [app_name: :flume]
      ])
    }

    Flume.Supervisor.start_link(config)
  end
end
