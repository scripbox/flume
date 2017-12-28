defmodule Flume.Queue.Manager do

  alias Flume.Redis.Job

  def enqueue(namespace, queue, worker, args) do
    job = serialized_job(queue, worker, args)
    Job.enqueue(Flume.Redis, namespace, queue, job)
  end

  def remove_job(namespace, queue, job) do
    Job.remove_job(Flume.Redis, namespace, queue, job)
  end

  def fetch_jobs(namespace, queue, count) do
    Job.dequeue_bulk(Flume.Redis, namespace, queue, count)
  end

  defp serialized_job(queue, worker, args) do
    jid = UUID.uuid4
    job = %{queue: queue, worker: worker, jid: jid, args: args, enqueued_at: :erlang.system_time(:seconds)}
    Poison.encode!(job)
  end
end
