defmodule Flume.Redis.Job do

  alias Flume.Redis.Client

  def enqueue(redis_conn, namespace, queue, serialized_job) do
    Client.lpush(redis_conn, queue_key(namespace, queue), serialized_job)
  end

  def dequeue(redis_conn, namespace, queue, job) do
    Client.lrem!(redis_conn, queue_key(namespace, queue), job)
  end

  defp queue_key(namespace, queue) do
    "#{namespace}:#{queue}"
  end
end
