defmodule TestWithRedis do
  use ExUnit.CaseTemplate

  alias Flume.Config

  setup _tags do
    on_exit(fn ->
      pool_size = Config.redis_pool_size()
      conn_key = :"#{Flume.Redis.Supervisor.redix_worker_prefix()}_#{pool_size - 1}"
      keys = Redix.command!(conn_key, ["KEYS", "#{Config.namespace()}:*"])
      Enum.map(keys, fn key -> Redix.command(conn_key, ["DEL", key]) end)
    end)

    :ok
  end

  def serialized_jobs(module_name, count),
    do: Enum.map(1..count, fn _ -> serialized_job(module_name) end)

  def serialized_job(module_name, args \\ []) do
    %{
      class: module_name,
      function: "perform",
      queue: "test",
      jid: "1082fd87-2508-4eb4-8fba-#{:rand.uniform(9_999_999)}a60e3",
      args: args,
      retry_count: 0,
      enqueued_at: 1_514_367_662,
      finished_at: nil,
      failed_at: nil,
      retried_at: nil,
      error_message: nil,
      error_backtrace: nil
    }
    |> Jason.encode!()
  end
end
