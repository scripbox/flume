defmodule Flume.Config do
  @defaults %{
    backoff_initial: 500,
    backoff_max: 10_000,
    database: 0,
    host: "127.0.0.1",
    logger: Flume.DefaultLogger,
    max_retries: 5,
    name: Flume,
    namespace: "flume",
    password: nil,
    pipelines: [],
    poll_timeout: 500,
    port: 6379,
    reconnect_on_sleep: 100,
    redis_pool_size: 10,
    redis_timeout: 5000,
    scheduler_poll_timeout: 10_000,
    start_on_application: true,
    instrumentation: [
      handler_module: Flume.Instrumentation.DefaultEventHandler,
      handler_function: :handle,
      config: [app_name: :flume]
    ]
  }

  @integer_keys [
    :port,
    :database,
    :reconnect_on_sleep,
    :redis_timeout,
    :poll_timeout,
    :scheduler_poll_timeout,
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

  def redis_opts do
    [host: host(), port: port(), database: database(), password: password()]
  end

  def connection_opts do
    [backoff: reconnect_on_sleep(), timeout: redis_timeout()]
  end

  def server_opts do
    [
      namespace: namespace(),
      poll_timeout: poll_timeout(),
      scheduler_poll_timeout: scheduler_poll_timeout()
    ]
  end

  def queues, do: Enum.map(pipelines(), & &1.queue)

  def pipeline_names, do: Enum.map(pipelines(), & &1.name)
end
