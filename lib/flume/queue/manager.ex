defmodule Flume.Queue.Manager do
  require Logger

  alias Flume.Config
  alias Flume.Redis.Job
  alias Flume.Queue.Backoff
  alias Flume.Support.Time
  alias Flume.Event

  def enqueue(namespace, queue, worker, args) do
    job = serialized_job(queue, worker, args)
    Job.enqueue(Flume.Redis, queue_key(namespace, queue), job)
  end

  def enqueue_in(namespace, queue, time_in_seconds, worker, args) do
    queue_name = queue_key(namespace, queue)
    job = serialized_job(queue, worker, args)

    schedule_job_at(queue_name, time_in_seconds, job)
  end

  def fetch_jobs(namespace, queue, count) do
    Job.bulk_dequeue(Flume.Redis, queue_key(namespace, queue), backup_key(namespace, queue), count)
  end

  def retry_or_fail_job(namespace, queue, serialized_job, error) do
    deserialized_job = Event.decode!(serialized_job)
    retry_count = deserialized_job.retry_count || 0
    response =
      if retry_count < Config.get(:max_retries) do
        retry_job(namespace, queue, deserialized_job, error, retry_count + 1)
      else
        Logger.info("Max retires on job #{deserialized_job.jid} exceeded")
        fail_job(namespace, queue, deserialized_job, error)
      end

    case response do
      {:ok, _} ->
        remove_retry(namespace, queue, deserialized_job.original_json)
        remove_job(backup_key(namespace, queue), deserialized_job.original_json)
      {:error, _} ->
        Logger.info("Failed to move job to a retry or dead queue.")
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
    schedule_job_at(retry_key(namespace, queue), retry_at, Poison.encode!(job))
  end

  def fail_job(namespace, queue, job, error) do
    job = %{
      job
      |
        retry_count: job.retry_count || 0,
        failed_at: Time.unix_seconds,
        error_message: error
    }
    Job.fail_job!(Flume.Redis, dead_key(namespace, queue), Poison.encode!(job))
    {:ok, nil}
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error("[#{namespace}-#{queue}] Job: #{job} failed with error: #{e.message}")
      {:error, e.reason}
  end

  def remove_job(queue, job) do
    count = Job.remove_job!(Flume.Redis, queue, job)
    {:ok, count}
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error("[#{queue}] Job: #{job} failed with error: #{e.message}")
      {:error, e.reason}
  end

  def remove_job(namespace, queue, job) do
    queue_key = queue_key(namespace, queue)
    count = Job.remove_job!(Flume.Redis, queue_key, job)
    {:ok, count}
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error("[#{namespace}-#{queue}] Job: #{job} failed with error: #{e.message}")
      {:error, e.reason}
  end

  def remove_retry(namespace, queue, job) do
    queue_key = retry_key(namespace, queue)
    count = Job.remove_scheduled_job!(Flume.Redis, queue_key, job)
    {:ok, count}
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error("[#{namespace}-#{queue}] Job: #{job} failed with error: #{e.message}")
      {:error, e.message}
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

  defp schedule_job_at(queue, time_in_seconds, job) do
    Job.schedule_job(Flume.Redis, queue, time_in_seconds, job)
  end

  defp serialized_job(queue, worker, args) do
    %Event{
      queue: queue,
      class: worker,
      jid: UUID.uuid4,
      args: args,
      enqueued_at: Time.unix_seconds,
      retry_count: 0
    } |> Poison.encode!()
  end

  defp next_time_to_retry(retry_count) do
    retry_count
    |> Backoff.calc_next_backoff()
    |> Time.offset_from_now()
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
