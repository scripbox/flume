defmodule Flume.Redis.Client do
  alias Flume.Config

  @pool_size Config.redis_pool_size()

  def lpush(key, value) do
    query(["LPUSH", key, value])
  end

  def lrem!(key, value, count \\ 1) do
    {:ok, res} = query(["LREM", key, count, value])
    res
  end

  def lrange!(key, range_start \\ 0, range_end \\ -1) do
    {:ok, res} = query(["LRANGE", key, range_start, range_end])
    res
  end

  def zadd(key, score, value) do
    query(["ZADD", key, score, value])
  end

  def zadd!(key, score, value) do
    {:ok, res} = zadd(key, score, value)
    res
  end

  def zrem!(set, member) do
    {:ok, res} = query(["ZREM", set, member])
    res
  end

  def zrange!(key, range_start \\ 0, range_end \\ -1) do
    {:ok, res} = query(["ZRANGE", key, range_start, range_end])
    res
  end

  def query(command) do
    Redix.command(redix_worker_name(), command)
  end

  def pipeline(command) do
    Redix.pipeline(redix_worker_name(), command)
  end

  # Private API
  defp random_index() do
    rem(System.unique_integer([:positive]), @pool_size)
  end

  defp redix_worker_name do
    :"#{Flume.redix_worker_prefix()}_#{random_index()}"
  end
end
