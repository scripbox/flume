defmodule Flume.Redis.SortedSet do
  alias Flume.Redis.Client

  defdelegate add(key, score, value), to: Client, as: :zadd

  defdelegate add!(key, score, value), to: Client, as: :zadd!

  defdelegate remove!(key, value), to: Client, as: :zrem!

  defdelegate fetch_by_range!(key, start_range \\ 0, end_range \\ 0), to: Client, as: :zrange!
end
