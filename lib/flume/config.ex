defmodule Flume.Config do
  @default_config %{
    name: Flume,
    host: "127.0.0.1",
    port: 6379,
    namespace: "flume",
    database: 0,
    redis_timeout: 5000,
    reconnect_on_sleep: 100,
    pipelines: []
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

    [backoff: reconnect_on_sleep, timeout: timeout, name: Flume.Redis]
  end

  # Private API
  defp port, do: to_integer(get(:port))

  defp database, do: to_integer(get(:database))

  defp to_integer(value) when is_binary(value) do
    String.to_integer(value)
  end
  defp to_integer(value), do: value
end
