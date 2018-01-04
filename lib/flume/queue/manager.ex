defmodule Flume.Queue.Manager do
  require Logger

  alias Flume.Config
  alias Flume.Redis.Job
  alias Flume.Queue.Backoff
  alias Flume.Support.{Time, JobSerializer}

  def enqueue(namespace, queue, worker, args) do
    job = serialized_job(queue, worker, args)
    Job.enqueue(Flume.Redis, queue_key(namespace, queue), job)
  end

  def fetch_jobs(namespace, queue, count) do
    Job.dequeue_bulk(Flume.Redis, queue_key(namespace, queue), backup_key(namespace, queue), count)
  end

  def retry_or_fail_job(namespace, queue, serialized_job, error, count \\ 0) do
    deserialized_job = JobSerializer.decode!(serialized_job)
    if count <= Config.get(:max_retries) do
      retry_job(namespace, queue, deserialized_job, error, count + 1)
    else
      Logger.info("Max retires on job #{deserialized_job.jid} exceeded")
      fail_job(namespace, queue, deserialized_job, error)
    end
  end

  def retry_job(namespace, queue, deserialized_job, error, count) do
    job = %{
      deserialized_job
      |
        retry_count: count,
        failed_at: Time.unix_seconds,
        error_message: error
    }
    retry_at = next_time_to_retry(count)
    enqueue_job_at(retry_key(namespace, queue), job.jid, JobSerializer.encode!(job), retry_at)
  end

  def fail_job(namespace, queue, job, error) do
    job = %{
      job
      |
        retry_count: job.count || 0,
        failed_at: Time.unix_seconds,
        error_message: error
    }
    Job.fail_job!(Flume.Redis, dead_key(namespace, queue), JobSerializer.encode!(job))
  end

  def enqueue_job_at(queue_key, jid, job, schedule_at) do
    Job.schedule_job(Flume.Redis, queue_key, jid, job, schedule_at)
  end

  def remove_job(namespace, queue, jid) do
    queue_key = queue_key(namespace, queue)
    job = find_job(queue_key, jid)
    Job.remove_job!(Flume.Redis, queue_key, job)
  end

  def remove_retry(namespace, queue, jid) do
    queue_key = retry_key(namespace, queue)
    job = find_job(queue_key, :retry, jid)
    Job.remove_retry_job(Flume.Redis, queue_key, job)
  end

  defp serialized_job(queue, worker, args) do
    jid = UUID.uuid4
    job = %{
      queue: queue,
      class: worker,
      jid: jid,
      args: args,
      enqueued_at: Time.unix_seconds,
      retry_count: 0
    }
    JobSerializer.encode!(job)
  end

  defp next_time_to_retry(retry_count) do
    retry_count
    |> Backoff.calc_next_backoff()
    |> Time.offset_from_now()
  end

  defp find_job(queue, jid) do
    Job.fetch_all!(Flume.Redis, queue)
    |> Enum.find(&(JobSerializer.decode!(&1).jid == jid))
  end

  defp find_job(queue, :retry = type, jid) do
    Job.fetch_all!(Flume.Redis, type, queue)
    |> Enum.find(&(JobSerializer.decode!(&1).jid == jid))
  end

  defp queue_key(namespace, queue), do: "#{namespace}:#{queue}"

  defp backup_key(namespace, queue), do: "#{namespace}:backup:#{queue}"

  defp retry_key(namespace, queue), do: "#{namespace}:retry:#{queue}"

  defp dead_key(namespace, queue), do: "#{namespace}:dead:#{queue}"

  defp scheduled_key(namespace, queue), do: "#{namespace}:scheduled:#{queue}"

  defp dead_key(namespace, queue) do
    "#{namespace}:dead:#{queue}"
  end
end
