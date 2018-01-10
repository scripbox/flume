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
    Job.bulk_dequeue(Flume.Redis, queue_key(namespace, queue), backup_key(namespace, queue), count)
  end

  def retry_or_fail_job(namespace, queue, serialized_job, error) do
    deserialized_job = JobSerializer.decode!(serialized_job)
    retry_count = deserialized_job.retry_count || 0
    if retry_count <= Config.get(:max_retries) do
      retry_job(namespace, queue, deserialized_job, error, retry_count + 1)
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
    Job.remove_scheduled_job!(Flume.Redis, queue_key, job)
  end

  @doc """
  Retrieves all the scheduled and retry jobs from the redis sorted set
  based on the queue name and max score and enqueues them into the main
  queue which will be processed.

  Returns {:ok, count}

  ## Examples

      iex> Flume.Queue.Manager.remove_and_enqueue_scheduled_jobs('flume_test', ['test'], "1515224298.912696")
      {:ok, 0}

  """
  def remove_and_enqueue_scheduled_jobs(namespace, queues, max_score) do
    scheduled_queues_and_jobs = Enum.map(queues, fn(queue) ->
      queue_keys = scheduled_keys(namespace, queue)
      responses = Job.scheduled_jobs(Flume.Redis, queue_keys, max_score)
      {queue, queue_keys |> Enum.zip(responses)}
    end)

    count = enqueue_jobs(namespace, scheduled_queues_and_jobs)
    {:ok, count}
  end

  defp enqueue_jobs(_namespace, []), do: 0
  defp enqueue_jobs(namespace, [{queue_name, responses}|other_jobs]) do
    enqueue_jobs(namespace, queue_name, responses) + enqueue_jobs(namespace, other_jobs)
  end

  defp enqueue_jobs(_namespace, _queue_name, []), do: 0
  defp enqueue_jobs(namespace, queue_name, [{scheduled_queue_name, jobs}|other_jobs]) do
    enqueued_job_count = enqueue_jobs(namespace, queue_name, scheduled_queue_name, jobs)
    enqueued_job_count + enqueue_jobs(namespace, queue_name, other_jobs)
  end

  defp enqueue_jobs(_namespace, _queue_name, _scheduled_queue_name, []), do: 0
  defp enqueue_jobs(namespace, queue_name, scheduled_queue_name, jobs) do
    enqueued_jobs = Job.bulk_enqueue!(Flume.Redis, queue_key(namespace, queue_name), jobs)
    Job.bulk_remove_scheduled!(Flume.Redis, scheduled_queue_name, enqueued_jobs) |> Enum.count
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

  defp scheduled_keys(namespace, queue) do
    [scheduled_key(namespace, queue), retry_key(namespace, queue)]
  end
end
