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

  def remove_job(namespace, queue, serialized_job) do
    Job.remove_job(Flume.Redis, queue_key(namespace, queue), serialized_job)
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
    Job.fail_job(Flume.Redis, dead_key(namespace, queue), JobSerializer.encode!(job))
  end

  def enqueue_job_at(queue_key, jid, job, schedule_at) do
    Job.schedule_job(Flume.Redis, queue_key, jid, job, schedule_at)
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

  defp queue_key(namespace, queue) do
    "#{namespace}:#{queue}"
  end

  defp backup_key(namespace, queue) do
    "#{namespace}:backup:#{queue}"
  end

  defp retry_key(namespace, queue) do
    "#{namespace}:retry:#{queue}"
  end

  defp dead_key(namespace, queue) do
    "#{namespace}:dead:#{queue}"
  end
end
