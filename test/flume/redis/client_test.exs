defmodule Flume.Redis.ClientTest do
  use Flume.TestWithRedis

  setup_all _redis do
    on_exit(fn ->
      clear_redis("flume")
    end)

    :ok
  end

  doctest Flume.Redis.Client
end
