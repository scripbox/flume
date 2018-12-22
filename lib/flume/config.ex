defmodule Flume.Config do
  @default_config %{
    name: Flume,
    host: "127.0.0.1",
    port: 6379,
    namespace: "flume",
    database: 0,
    redis_timeout: 5000,
    redis_pool_size: 10,
    reconnect_on_sleep: 100,
    poll_timeout: 500,
    pipelines: [],
    backoff_initial: 500,
    backoff_max: 10_000,
    scheduler_poll_timeout: 10_000,
    logger: Flume.DefaultLogger
  }

  def get(key) do
    get(key, Map.get(@default_config, key))
  end

  def get(key, fallback) do
    case Application.get_env(:flume, key, fallback) do
      {:system, varname} -> System.get_env(varname)
      {:system, varname, default} -> System.get_env(varname) || default
      value -> value
    end
  end

  def redis_opts do
    host = get(:host)
    port = get(:port) |> to_integer
    database = get(:database) |> to_integer
    password = get(:password)

    [host: host, port: port, database: database, password: password]
  end

  def connection_opts do
    reconnect_on_sleep = get(:reconnect_on_sleep) |> to_integer
    timeout = get(:redis_timeout) |> to_integer

    [backoff: reconnect_on_sleep, timeout: timeout]
  end

  def redis_pool_size do
    get(:redis_pool_size)
  end

  def server_opts do
    namespace = get(:namespace)
    poll_timeout = get(:poll_timeout) |> to_integer
    scheduler_poll_timeout = get(:scheduler_poll_timeout) |> to_integer

    [
      namespace: namespace,
      poll_timeout: poll_timeout,
      scheduler_poll_timeout: scheduler_poll_timeout
    ]
  end

  def queues, do: Enum.map(get(:pipelines), & &1.queue)

  def backoff_initial, do: get(:backoff_initial) |> to_integer

  def backoff_max, do: get(:backoff_max) |> to_integer

  def scheduler_poll_timeout, do: get(:scheduler_poll_timeout) |> to_integer

  defp to_integer(value) when is_binary(value) do
    String.to_integer(value)
  end

  defp to_integer(value), do: value

  def logger, do: get(:logger)
end
