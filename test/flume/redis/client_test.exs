defmodule Flume.Redis.ClientTest do
  use TestWithRedis

  setup_all _redis do
    on_exit(fn ->
      pool_size = Flume.Config.redis_pool_size()
      conn = :"#{Flume.Redis.Supervisor.redix_worker_prefix()}_#{pool_size - 1}"
      keys = Redix.command!(conn, ["KEYS", "flume:test:*"])

      Enum.map(keys, fn key -> ["DEL", key] end)
      |> case do
        [] ->
          []

        commands ->
          Redix.pipeline(conn, commands)
      end
    end)

    :ok
  end

  doctest Flume.Redis.Client
end
