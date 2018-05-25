defmodule Flume.Redis.Client do
  alias Flume.Config

  @pool_size Config.redis_pool_size()

  # Redis commands
  @lpush "LPUSH"
  @lrem "LREM"
  @lrange "LRANGE"
  @zadd "ZADD"
  @zrem "ZREM"
  @zrange "ZRANGE"
  @keys "KEYS"
  @smembers "SMEMBERS"
  @sadd "SADD"
  @del "DEL"
  @hgetall "HGETALL"

  def lpush(key, value) do
    query([@lpush, key, value])
  end

  def lrem!(key, value, count \\ 1) do
    {:ok, res} = query([@lrem, key, count, value])
    res
  end

  def lrange!(key, range_start \\ 0, range_end \\ -1) do
    {:ok, res} = query([@lrange, key, range_start, range_end])
    res
  end

  def zadd(key, score, value) do
    query([@zadd, key, score, value])
  end

  def zadd!(key, score, value) do
    {:ok, res} = zadd(key, score, value)
    res
  end

  def zrem!(set, member) do
    {:ok, res} = query([@zrem, set, member])
    res
  end

  def zrange!(key, range_start \\ 0, range_end \\ -1) do
    {:ok, res} = query([@zrange, key, range_start, range_end])
    res
  end

  def keys(pattern) do
    {:ok, res} = query([@keys, pattern])
    res
  end

  def smembers(key) do
    {:ok, res} = query([@smembers, key])
    res
  end

  def hgetall(key) do
    {:ok, res} = query([@hgetall, key])
    res
  end

  def sadd(key, value) do
    {:ok, res} = query([@sadd, key, value])
    res
  end

  def del(key) do
    {:ok, res} = query([@del, key])
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
