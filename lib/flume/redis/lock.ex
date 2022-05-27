defmodule Flume.Redis.Lock do
  require Flume.Logger

  alias Flume.Redis.{Client, Script}

  @release_lock_script Script.compile(:release_lock)

  def acquire(
        lock_key,
        ttl
      ) do
    token = UUID.uuid4()

    case Client.set_nx(lock_key, token, ttl) do
      {:ok, "OK"} ->
        {:ok, token}

      {:ok, nil} ->
        {:error, :locked}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def release(lock_key, token) do
    response =
      Script.eval(@release_lock_script, [
        _num_of_keys = 1,
        lock_key,
        token
      ])

    case response do
      {:ok, _count} ->
        :ok

      {:error, val} ->
        {:error, val}
    end
  end
end
