defmodule Flume.Redis.LockTest do
  use TestWithRedis

  alias Flume.Redis.Lock

  @namespace Flume.Config.namespace()
  @lock_key "#{@namespace}:acquire_lock_test"

  describe "acquire_lock/3" do
    test "mutual exclusion" do
      ttl = 10_000

      Enum.each(1..3, fn _ ->
        processes = 1..10

        locks_acquired =
          Enum.map(processes, fn _ ->
            Task.async(fn ->
              Lock.acquire(@lock_key, ttl)
            end)
          end)
          |> Enum.map(&Task.await(&1, 1000))
          |> Keyword.delete(:error)

        assert length(locks_acquired) == 1
        assert Lock.release(@lock_key, locks_acquired[:ok]) == :ok
      end)
    end

    test "lock expires after ttl" do
      ttl = 1000
      {:ok, _} = Lock.acquire(@lock_key, ttl)
      Process.sleep(1100)
      {:ok, token} = Lock.acquire(@lock_key, ttl)
      assert Lock.release(@lock_key, token) == :ok
    end

    test "one process cannot clean another process's lock" do
      ttl = 10_000
      {:ok, _token} = Lock.acquire(@lock_key, ttl)
      assert Lock.release(@lock_key, "other_token") == :ok
      assert {:error, :locked} = Lock.acquire(@lock_key, ttl)
    end
  end
end
