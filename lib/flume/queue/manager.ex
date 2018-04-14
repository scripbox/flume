defmodule Flume.Queue.Manager do
  require Logger

  alias Flume.Config
  alias Flume.Redis.Job
  alias Flume.Queue.Backoff
  alias Flume.Support.Time
  alias Flume.Event

  def enqueue(namespace, queue, worker, function_name, args) do
    job = serialized_job(queue, worker, function_name, args)
    Job.enqueue(queue_key(namespace, queue), job)
  end

  def enqueue_in(namespace, queue, time_in_seconds, worker, function_name, args) do
    queue_name = scheduled_key(namespace)
    job = serialized_job(queue, worker, function_name, args)

    schedule_job_at(queue_name, time_in_seconds, job)
  end

  def fetch_jobs(namespace, queue, count) do
    Job.bulk_dequeue(
      queue_key(namespace, queue),
      backup_key(namespace, queue),
      count
    )
  end

  def retry_or_fail_job(namespace, queue, serialized_job, error) do
    deserialized_job = Event.decode!(serialized_job)
    retry_count = deserialized_job.retry_count || 0

    response =
      if retry_count < Config.get(:max_retries) do
        retry_job(namespace, deserialized_job, error, retry_count + 1)
      else
        Logger.info("Max retires on job #{deserialized_job.jid} exceeded")
        fail_job(namespace, deserialized_job, error)
      end

    case response do
      {:ok, value} ->
        remove_retry(namespace, deserialized_job.original_json)
        remove_job(backup_key(namespace, queue), deserialized_job.original_json)
        {:ok, value}

      {:error, message} ->
        Logger.info("Failed to move job to a retry or dead queue.")
        {:error, message}
    end
  end

  def retry_job(namespace, deserialized_job, error, count) do
    job = %{
      deserialized_job
      | retry_count: count,
        failed_at: Time.unix_seconds(),
        error_message: error
    }

    retry_at = next_time_to_retry(count)
    schedule_job_at(retry_key(namespace), retry_at, Poison.encode!(job))
  end

  def fail_job(namespace, job, error) do
    job = %{
      job
      | retry_count: job.retry_count || 0,
        failed_at: Time.unix_seconds(),
        error_message: error
    }

    Job.fail_job!(dead_key(namespace), Poison.encode!(job))
    {:ok, nil}
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error("[#{dead_key(namespace)}] Job: #{job} failed with error: #{e.message}")
      {:error, e.reason}
  end

  def remove_job(queue, job) do
    count = Job.remove_job!(queue, job)
    {:ok, count}
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error("[#{queue}] Job: #{job} failed with error: #{e.message}")
      {:error, e.reason}
  end

  def remove_job(namespace, queue, job) do
    queue_key = queue_key(namespace, queue)
    count = Job.remove_job!(queue_key, job)
    {:ok, count}
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error("[#{queue_key(namespace, queue)}] Job: #{job} failed with error: #{e.message}")
      {:error, e.reason}
  end

  def remove_retry(namespace, job) do
    queue_key = retry_key(namespace)
    count = Job.remove_scheduled_job!(queue_key, job)
    {:ok, count}
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error("[#{retry_key(namespace)}] Job: #{job} failed with error: #{e.message}")
      {:error, e.message}
  end

  def remove_backup(namespace, queue, job) do
    queue_key = backup_key(namespace, queue)
    count = Job.remove_job!(queue_key, job)
    {:ok, count}
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error(
        "[#{backup_key(namespace, queue)}] Job: #{job} failed with error: #{e.message}"
      )

      {:error, e.reason}
  end

  def remove_backup_jobs(namespace, jobs) do
    jobs =
      jobs |> Enum.map(fn job ->
        deserialized_job = Event.decode!(job)
        [backup_key(namespace, deserialized_job.queue), job]
      end)
    Job.remove_jobs(jobs)
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error(
        "[remove_backup_jobs]: #{jobs |> length} jobs failed with error: #{e.message}"
      )

      {:error, e.reason}
  end

  @doc """
  Retrieves all the scheduled and retry jobs from the redis sorted set
  based on the queue name and max score and enqueues them into the main
  queue which will be processed.

  Returns {:ok, count}

  ## Examples

      iex> Flume.Queue.Manager.remove_and_enqueue_scheduled_jobs('flume_test', "1515224298.912696")
      {:ok, 0}

  """
  def remove_and_enqueue_scheduled_jobs(namespace, max_score) do
    scheduled_queues = scheduled_keys(namespace)

    case Job.scheduled_jobs(scheduled_queues, max_score) do
      {:error, error_message} ->
        {:error, error_message}

      {:ok, scheduled_queues_and_jobs} ->
        if Enum.all?(scheduled_queues_and_jobs, fn {_, jobs} -> Enum.empty?(jobs) end) do
          {:ok, 0}
        else
          enqueued_jobs = enqueue_scheduled_jobs(namespace, scheduled_queues_and_jobs)
          count = Job.bulk_remove_scheduled!(enqueued_jobs) |> Enum.count()
          {:ok, count}
        end
    end
  end

  def enqueue_scheduled_jobs(namespace, scheduled_queues_and_jobs) do
    queues_and_jobs =
      scheduled_queues_and_jobs
      |> Enum.flat_map(fn {scheduled_queue, jobs} ->
        Enum.map(jobs, fn job ->
          deserialized_job = Event.decode!(job)
          {scheduled_queue, queue_key(namespace, deserialized_job.queue), job}
        end)
      end)

    Job.bulk_enqueue_scheduled!(queues_and_jobs)
  end

  defp schedule_job_at(queue, retry_at, job) do
    Job.schedule_job(queue, retry_at, job)
  end

  defp serialized_job(queue, worker, function_name, args) do
    %Event{
      queue: queue,
      class: worker,
      function: function_name,
      jid: UUID.uuid4(),
      args: args,
      enqueued_at: Time.unix_seconds(),
      retry_count: 0
    }
    |> Poison.encode!()
  end

  defp next_time_to_retry(retry_count) do
    retry_count
    |> Backoff.calc_next_backoff()
    |> Time.offset_from_now()
    |> Time.unix_seconds()
  end

  defp full_key(namespace, key), do: "#{namespace}:#{key}"

  defp queue_key(namespace, queue), do: full_key(namespace, "queue:#{queue}")

  defp backup_key(namespace, queue), do: full_key(namespace, "queue:backup:#{queue}")

  defp retry_key(namespace), do: full_key(namespace, "retry")

  defp dead_key(namespace), do: full_key(namespace, "dead")

  defp scheduled_key(namespace), do: full_key(namespace, "scheduled")

  defp scheduled_keys(namespace) do
    [scheduled_key(namespace), retry_key(namespace)]
  end
end
