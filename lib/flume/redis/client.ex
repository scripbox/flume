defmodule Flume.Redis.Client do
  def lpush(conn, key, value) do
    query(conn, ["LPUSH", key, value])
  end

  def lrem!(conn, key, value, count \\ 1) do
    {:ok, res} = query(conn, ["LREM", key, count, value])
    res
  end

  def query(conn, command) do
    Redix.command(conn, command)
  end
end
