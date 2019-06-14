defmodule Flume.Queue.Manager do
  require Flume.Logger

  alias Flume.{Config, Event, Logger, Instrumentation, Utils}
  alias Flume.Redis.Job
  alias Flume.Queue.Backoff
  alias Flume.Support.Time, as: TimeExtension

  @external_resource "priv/scripts/enqueue_processing_jobs.lua"
  @external_resource "priv/scripts/release_lock.lua"

  def enqueue(
        namespace,
        queue,
        worker,
        function_name,
        args,
        opts \\ []
      ) do
    job = serialized_job(queue, worker, function_name, args, opts[:context])
    queue_atom = if is_atom(queue), do: queue, else: String.to_atom(queue)

    Instrumentation.execute(
      [queue_atom, :enqueue],
      %{payload_size: byte_size(job)},
      true
    )

    Job.enqueue(queue_key(namespace, queue), job)
  end

  def bulk_enqueue(namespace, queue, jobs, opts \\ []) do
    jobs =
      jobs
      |> Enum.map(fn
        [worker, function_name, args] ->
          serialized_job(queue, worker, function_name, args, opts[:context])

        [worker, args] ->
          serialized_job(queue, worker, :perform, args, opts[:context])
      end)

    queue_atom = if is_atom(queue), do: queue, else: String.to_atom(queue)

    Instrumentation.execute(
      [queue_atom, :enqueue],
      %{payload_size: Utils.payload_size(jobs)},
      true
    )

    Job.bulk_enqueue(queue_key(namespace, queue), jobs)
  end

  def enqueue_in(
        namespace,
        queue,
        time_in_seconds,
        worker,
        function_name,
        args,
        opts \\ []
      ) do
    queue_name = scheduled_key(namespace)
    job = serialized_job(queue, worker, function_name, args, opts[:context])

    schedule_job_at(queue_name, time_in_seconds, job)
  end

  def fetch_jobs(
        namespace,
        queue,
        count,
        %{rate_limit_count: rate_limit_count, rate_limit_scale: rate_limit_scale} =
          rate_limit_opts
      ) do
    {current_score, previous_score} = current_and_previous_score(rate_limit_scale)

    Job.bulk_dequeue(
      queue_key(namespace, queue),
      processing_key(namespace, queue),
      rate_limit_key(namespace, queue, rate_limit_opts[:rate_limit_key]),
      count,
      rate_limit_count,
      previous_score,
      current_score
    )
  end

  def fetch_jobs(namespace, queue, count) do
    Job.bulk_dequeue(
      queue_key(namespace, queue),
      processing_key(namespace, queue),
      count,
      TimeExtension.time_to_score()
    )
  end

  def enqueue_processing_jobs(namespace, utc_time, queue) do
    Job.enqueue_processing_jobs(
      processing_key(namespace, queue),
      queue_key(namespace, queue),
      TimeExtension.time_to_score(utc_time)
    )
  end

  def retry_or_fail_job(namespace, queue, serialized_job, error) do
    deserialized_job = Event.decode!(serialized_job)
    retry_count = deserialized_job.retry_count || 0

    response =
      if retry_count < Config.max_retries() do
        retry_job(namespace, deserialized_job, error, retry_count + 1)
      else
        Logger.info("Max retires on job #{deserialized_job.jid} exceeded")
        fail_job(namespace, deserialized_job, error)
      end

    case response do
      {:ok, _} ->
        remove_retry(namespace, deserialized_job.original_json)
        remove_processing(namespace, queue, deserialized_job.original_json)

      {:error, _} ->
        Logger.info("Failed to move job to a retry or dead queue.")
    end
  end

  def retry_job(namespace, deserialized_job, error, count) do
    job = %{
      deserialized_job
      | retry_count: count,
        failed_at: TimeExtension.unix_seconds(),
        error_message: error
    }

    retry_at = next_time_to_retry(count)
    schedule_job_at(retry_key(namespace), retry_at, Jason.encode!(job))
  end

  def fail_job(namespace, job, error) do
    job = %{
      job
      | retry_count: job.retry_count || 0,
        failed_at: TimeExtension.unix_seconds(),
        error_message: error
    }

    Job.fail_job!(dead_key(namespace), Jason.encode!(job))
    {:ok, nil}
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error("[#{dead_key(namespace)}] Job: #{job} failed with error: #{Kernel.inspect(e)}")
      {:error, e.reason}
  end

  def remove_job(queue, job) do
    count = Job.remove_job!(queue, job)
    {:ok, count}
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error("[#{queue}] Job: #{job} failed with error: #{Kernel.inspect(e)}")
      {:error, e.reason}
  end

  def remove_job(namespace, queue, job) do
    queue_key = queue_key(namespace, queue)
    count = Job.remove_job!(queue_key, job)
    {:ok, count}
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error(
        "[#{queue_key(namespace, queue)}] Job: #{job} failed with error: #{Kernel.inspect(e)}"
      )

      {:error, e.reason}
  end

  def remove_retry(namespace, job) do
    queue_key = retry_key(namespace)
    count = Job.remove_scheduled_job!(queue_key, job)
    {:ok, count}
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error(
        "[#{retry_key(namespace)}] Job: #{job} failed with error: #{Kernel.inspect(e)}"
      )

      {:error, e.message}
  end

  def remove_processing(namespace, queue, job) do
    processing_queue_key = processing_key(namespace, queue)
    count = Job.remove_processing!(processing_queue_key, job)
    {:ok, count}
  rescue
    e in [Redix.Error, Redix.ConnectionError] ->
      Logger.error("[#{queue}] Job: #{job} failed with error: #{Kernel.inspect(e)}")

      {:error, e.reason}
  end

  @doc """
  Retrieves all the scheduled and retry jobs from the redis sorted set
  based on the queue name and max score and enqueues them into the main
  queue which will be processed.
  ## Examples
      iex> Flume.Queue.Manager.remove_and_enqueue_scheduled_jobs('flume_test', "1515224298.912696")
      {:ok, 0}
  """
  # TODO: Refactor this!
  # This function and other functions related to this are complex because
  # we are trying to mix the processing of `scheduled` and `retry` jobs together.
  # This can be simplified by having two scheduler processes for each.
  def remove_and_enqueue_scheduled_jobs(namespace, max_score) do
    scheduled_keys(namespace)
    |> Job.scheduled_jobs(max_score)
    |> case do
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

  def queue_length(namespace, queue), do: queue_key(namespace, queue) |> Job.queue_length()

  defp schedule_job_at(queue, retry_at, job) do
    Job.schedule_job(queue, retry_at, job)
  end

  defp serialized_job(queue, worker, function_name, args, context) do
    %Event{
      queue: queue,
      class: worker,
      function: function_name,
      jid: UUID.uuid4(),
      args: args,
      enqueued_at: TimeExtension.unix_seconds(),
      retry_count: 0
    }
    |> add_context_to_event(context)
    |> Jason.encode!()
  end

  defp add_context_to_event(event, nil), do: event
  defp add_context_to_event(event, context) when context == %{}, do: event
  defp add_context_to_event(event, context), do: Map.put(event, :context, context)

  defp next_time_to_retry(retry_count) do
    retry_count
    |> Backoff.calc_next_backoff()
    |> TimeExtension.offset_from_now()
    |> TimeExtension.unix_seconds()
  end

  defp full_key(namespace, key), do: "#{namespace}:#{key}"

  defp queue_key(namespace, queue), do: full_key(namespace, "queue:#{queue}")

  defp retry_key(namespace), do: full_key(namespace, "retry")

  defp dead_key(namespace), do: full_key(namespace, "dead")

  defp scheduled_key(namespace), do: full_key(namespace, "scheduled")

  defp scheduled_keys(namespace) do
    [scheduled_key(namespace), retry_key(namespace)]
  end

  def processing_key(namespace, queue), do: full_key(namespace, "queue:processing:#{queue}")

  def rate_limit_key(namespace, queue, nil), do: full_key(namespace, "queue:limit:#{queue}")

  def rate_limit_key(namespace, _queue, key), do: full_key(namespace, "limit:#{key}")

  defp current_and_previous_score(offset_in_ms) do
    current_time = DateTime.utc_now()
    current_score = TimeExtension.unix_seconds(current_time)

    previous_score =
      TimeExtension.offset_before(offset_in_ms, current_time)
      |> TimeExtension.time_to_score()

    {current_score, previous_score}
  end
end
