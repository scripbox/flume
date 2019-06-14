defmodule Flume.Redis.Job do
  require Flume.Logger

  alias Flume.Logger
  alias Flume.Support.Time
  alias Flume.Redis.{Client, Script, SortedSet, BulkDequeue}

  @enqueue_processing_jobs_sha Script.sha(:enqueue_processing_jobs)

  def enqueue(queue_key, job) do
    try do
      response = Client.rpush(queue_key, job)

      case response do
        {:ok, [%Redix.Error{}, %Redix.Error{}]} = error -> error
        {:ok, [%Redix.Error{}, _]} = error -> error
        {:ok, [_, %Redix.Error{}]} = error -> error
        {:ok, [_, _]} -> :ok
        other -> other
      end
    catch
      :exit, e ->
        Logger.info("Error enqueueing -  #{Kernel.inspect(e)}")
        {:error, :timeout}
    end
  end

  def bulk_enqueue(queue_key, jobs) do
    Client.bulk_rpush(queue_key, jobs)
  end

  defdelegate bulk_dequeue(
                dequeue_key,
                processing_sorted_set_key,
                count,
                current_score
              ),
              to: BulkDequeue,
              as: :exec

  defdelegate bulk_dequeue(
                dequeue_key,
                processing_sorted_set_key,
                limit_sorted_set_key,
                count,
                max_count,
                previous_score,
                current_score
              ),
              to: BulkDequeue,
              as: :exec_rate_limited

  def enqueue_processing_jobs(sorted_set_key, queue_key, current_score) do
    Client.evalsha_command([
      @enqueue_processing_jobs_sha,
      _num_of_keys = 2,
      sorted_set_key,
      queue_key,
      current_score
    ])
    |> Client.query()
    |> case do
      {:error, reason} ->
        {:error, reason}

      {:ok, response} ->
        {:ok, response}
    end
  end

  def bulk_enqueue_scheduled!(queues_and_jobs) do
    group_by_queue(queues_and_jobs)
    |> Enum.map(fn {queue, scheduled_queues_and_jobs} ->
      jobs = Enum.map(scheduled_queues_and_jobs, fn {_, job} -> job end)
      response = bulk_enqueue(queue, jobs)
      {scheduled_queues_and_jobs, response}
    end)
    |> Enum.flat_map(fn {scheduled_queues_and_jobs, response} ->
      case response do
        {:error, error} ->
          Logger.error("Error running command - #{Kernel.inspect(error)}")
          []

        {:ok, _count} ->
          scheduled_queues_and_jobs
      end
    end)
  end

  def bulk_remove_scheduled!(scheduled_queues_and_jobs) do
    bulk_remove_scheduled_commands(scheduled_queues_and_jobs)
    |> Client.pipeline()
    |> case do
      {:error, reason} ->
        [{:error, reason}]

      {:ok, responses} ->
        scheduled_queues_and_jobs
        |> Enum.zip(responses)
        |> Enum.map(fn {{_, job}, response} ->
          case response do
            value when value in [:undefined, nil] ->
              nil

            error when error in [%Redix.Error{}, %Redix.ConnectionError{}] ->
              Logger.error("Error running command - #{Kernel.inspect(error)}")
              nil

            _value ->
              job
          end
        end)
        |> Enum.reject(&is_nil/1)
    end
  end

  def schedule_job(queue_key, schedule_at, job) do
    score =
      if is_float(schedule_at) do
        schedule_at |> Float.to_string()
      else
        schedule_at |> Integer.to_string()
      end

    try do
      case SortedSet.add(queue_key, score, job) do
        {:ok, jid} -> {:ok, jid}
        other -> other
      end
    catch
      :exit, e ->
        Logger.info("Error enqueueing -  #{Kernel.inspect(e)}")
        {:error, :timeout}
    end
  end

  def remove_job!(queue_key, job) do
    Client.lrem!(queue_key, job)
  end

  def remove_processing!(processing_sorted_set_key, job) do
    Client.zrem!(processing_sorted_set_key, job)
  end

  def remove_scheduled_job!(queue_key, job) do
    SortedSet.remove!(queue_key, job)
  end

  def fail_job!(queue_key, job) do
    SortedSet.add!(queue_key, Time.time_to_score(), job)
  end

  def fetch_all!(queue_key) do
    Client.lrange!(queue_key)
  end

  def fetch_all!(:retry, queue_key) do
    SortedSet.fetch_by_range!(queue_key)
  end

  def scheduled_jobs(queues, score) do
    Enum.map(queues, &["ZRANGEBYSCORE", &1, 0, score])
    |> Client.pipeline()
    |> case do
      {:error, reason} ->
        {:error, reason}

      {:ok, response} ->
        # TODO: Handle error response in response array
        updated_jobs =
          response
          |> Enum.map(fn jobs ->
            Enum.map(jobs, fn job ->
              case job do
                value when value in [:undefined, nil] ->
                  nil

                error when error in [%Redix.Error{}, %Redix.ConnectionError{}] ->
                  Logger.error("Error running command - #{Kernel.inspect(error)}")
                  nil

                value ->
                  value
              end
            end)
            |> Enum.reject(&is_nil/1)
          end)

        {:ok, Enum.zip(queues, updated_jobs)}
    end
  end

  def queue_length(queue_key), do: Client.llen(queue_key)

  defp group_by_queue(queues_and_jobs) do
    Enum.group_by(
      queues_and_jobs,
      fn {_scheduled_queue, queue_name, _job} -> queue_name end,
      fn {scheduled_queue, _queue_name, job} -> {scheduled_queue, job} end
    )
  end

  defp bulk_remove_scheduled_commands([]), do: []

  defp bulk_remove_scheduled_commands([{set_name, job} | scheduled_queues_and_jobs]) do
    cmd = Client.zrem_command(set_name, job)
    [cmd | bulk_remove_scheduled_commands(scheduled_queues_and_jobs)]
  end
end
