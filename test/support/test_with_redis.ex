defmodule TestWithRedis do
  use ExUnit.CaseTemplate

  alias Flume.Config

  setup _tags do
    on_exit(fn ->
      pool_size = Config.redis_pool_size()
      conn_key = :"#{Flume.redix_worker_prefix()}_#{pool_size - 1}"
      keys = Redix.command!(conn_key, ["KEYS", "#{Config.get(:namespace)}:*"])
      Enum.map(keys, fn key -> Redix.command(conn_key, ["DEL", key]) end)
    end)

    :ok
  end
end
