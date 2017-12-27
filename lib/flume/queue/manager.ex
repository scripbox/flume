defmodule Flume.Queue.Manager do

  alias Flume.Redis.Job

  def enqueue(namespace, queue, worker, args) do
    job = serialized_job(queue, worker, args)
    Job.enqueue(Flume.Redis, "#{namespace}:#{queue}", job)
  end

  def dequeue(namespace, queue, job) do
    Job.dequeue(Flume.Redis, "#{namespace}:#{queue}", job)
  end

  defp serialized_job(queue, worker, args) do
    jid = UUID.uuid4
    job = %{queue: queue, worker: worker, jid: jid, args: args, enqueued_at: :erlang.system_time(:seconds)}
    Poison.encode!(job)
  end
end
