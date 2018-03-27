defmodule Flume.Redis.Client do
  def lpush(conn, key, value) do
    query(conn, ["LPUSH", key, value])
  end

  def lrem!(conn, key, value, count \\ 1) do
    {:ok, res} = query(conn, ["LREM", key, count, value])
    res
  end

  def lrange!(conn, key, range_start \\ 0, range_end \\ -1) do
    {:ok, res} = query(conn, ["LRANGE", key, range_start, range_end])
    res
  end

  def zadd(conn, key, score, value) do
    query(conn, ["ZADD", key, score, value])
  end

  def zadd!(conn, key, score, value) do
    {:ok, res} = zadd(conn, key, score, value)
    res
  end

  def zrem!(conn, set, member) do
    {:ok, res} = query(conn, ["ZREM", set, member])
    res
  end

  def zrange!(conn, key, range_start \\ 0, range_end \\ -1) do
    {:ok, res} = query(conn, ["ZRANGE", key, range_start, range_end])
    res
  end

  def query(conn, command) do
    Redix.command(conn, command)
  end

  def pipeline(conn, command) do
    Redix.pipeline(conn, command)
  end
end
