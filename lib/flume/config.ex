defmodule Flume.Config do
  @default_config %{
    name: Flume,
    host: "127.0.0.1",
    port: 6379,
    namespace: "flume",
    database: 0,
    redis_timeout: 5000,
    reconnect_on_sleep: 100
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

  def port, do: to_integer(get(:port))

  def database, do: to_integer(get(:database))

  defp to_integer(value) when is_binary(value) do
    String.to_integer(value)
  end
  defp to_integer(value), do: value
end
