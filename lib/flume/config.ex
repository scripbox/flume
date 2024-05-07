defmodule Flume.Config do
  @defaults %{
    backoff_initial: 500,
    backoff_max: 10_000,
    database: 0,
    host: "127.0.0.1",
    logger: Flume.DefaultLogger,
    mock: false,
    max_retries: 5,
    name: Flume,
    namespace: "flume",
    password: nil,
    pipelines: [],
    port: 6379,
    redis_pool_size: 10,
    redis_timeout: 5000,
    scheduler_poll_interval: 10_000,
    dequeue_lock_ttl: 30_000,
    dequeue_process_timeout: 10_000,
    dequeue_lock_poll_interval: 500,
    debug_log: false,
    # In seconds
    visibility_timeout: 600,
    instrumentation: [
      handler_module: Flume.Instrumentation.DefaultEventHandler,
      handler_function: :handle,
      config: [app_name: :flume]
    ]
  }

  @integer_keys [
    :port,
    :database,
    :redis_timeout,
    :scheduler_poll_interval,
    :backoff_initial,
    :backoff_max
  ]

  alias Flume.Utils.IntegerExtension

  Map.keys(@defaults)
  |> Enum.each(fn key ->
    def unquote(key)(), do: get(unquote(key))
  end)

  def get(key), do: get(key, default(key))

  def get(key, fallback) do
    value =
      case Application.get_env(:flume, key, fallback) do
        {:system, varname} -> System.get_env(varname)
        {:system, varname, default} -> System.get_env(varname) || default
        value -> value
      end

    parse(key, value)
  end

  defp default(key), do: Map.get(@defaults, key)

  defp parse(key, value) when key in @integer_keys do
    case IntegerExtension.parse(value) do
      :error ->
        raise Flume.Errors.InvalidConfiguration, key

      parsed_value ->
        parsed_value
    end
  end

  defp parse(_key, value), do: value

  def redis_opts(opts) do
    [
      host: Keyword.get(opts, :host, host()),
      port: Keyword.get(opts, :port, port()),
      database: Keyword.get(opts, :database, database()),
      password: Keyword.get(opts, :password, password())
    ]
  end

  def connection_opts(opts) do
    [timeout: Keyword.get(opts, :timeout, redis_timeout())]
  end

  def scheduler_opts do
    [
      namespace: namespace(),
      scheduler_poll_interval: scheduler_poll_interval()
    ]
  end

  def queues, do: Enum.map(pipelines(), & &1.queue)

  def pipeline_names, do: Enum.map(pipelines(), & &1.name)

  def queue_api_module do
    case mock() do
      false ->
        Flume.Queue.DefaultAPI

      true ->
        Flume.Queue.MockAPI
    end
  end

  def pipeline_api_module do
    case mock() do
      false ->
        Flume.Pipeline.DefaultAPI

      true ->
        Flume.Pipeline.MockAPI
    end
  end
end
