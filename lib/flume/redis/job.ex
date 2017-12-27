defmodule Flume.Redis.Job do

  alias Flume.Redis.Client

  def enqueue(redis_conn, queue, serialized_job) do
    Client.lpush(redis_conn, queue, serialized_job)
  end

  def dequeue(redis_conn, queue, job) do
    Client.lrem!(redis_conn, queue, job)
  end
end
