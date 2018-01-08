defmodule TestWithRedis do
  use ExUnit.CaseTemplate

  alias Flume.Config

  setup _tags do
    on_exit fn ->
      keys = Redix.command!(Flume.Redis, ["KEYS", "#{Config.get(:namespace)}:*"])
      Enum.map(keys, fn(key) -> Redix.command(Flume.Redis, ["DEL", key]) end)
    end
    :ok
  end
end
