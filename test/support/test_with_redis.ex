defmodule Flume.TestWithRedis do
  use ExUnit.CaseTemplate, async: true

  using do
    quote do
      alias Flume.Config

      setup _tags do
        clear_redis()

        on_exit(fn ->
          clear_redis()
        end)

        :ok
      end

      def clear_redis(namespace \\ Config.namespace()) do
        pool_size = Config.redis_pool_size()
        conn = :"#{Flume.Redis.Supervisor.redix_worker_prefix()}_#{pool_size - 1}"
        keys = Redix.command!(conn, ["KEYS", "#{namespace}:*"])

        Enum.map(keys, fn key -> ["DEL", key] end)
        |> case do
          [] ->
            []

          commands ->
            Redix.pipeline(conn, commands)
        end
      end
    end
  end
end
