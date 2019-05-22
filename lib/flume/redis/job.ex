defmodule Flume.Redis.Job do
  require Flume.Logger

  alias Flume.Logger
  alias Flume.Support.Time
  alias Flume.Redis.{Client, Script, SortedSet}

  @bulk_dequeue_sha Script.sha(:bulk_dequeue)
  @bulk_dequeue_limited_sha Script.sha(:bulk_dequeue_limited)
  @enqueue_processing_jobs_sha Script.sha(:enqueue_processing_jobs)
  @bulk_dequeue_lock_prefix "bulk_dequeue_lock"

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
    jobs
    |> Enum.map(&Client.rpush_command(queue_key, &1))
    |> Client.pipeline()
    |> case do
      {:error, reason} ->
        {:error, reason}

      {:ok, reponses} ->
        updated_responses =
          reponses
          |> Enum.map(fn response ->
            case response do
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

        {:ok, updated_responses}
    end
  end

  def bulk_dequeue(dequeue_key, processing_sorted_set_key, count, current_score) do
    Client.evalsha_command([
      @bulk_dequeue_sha,
      _num_of_keys = 2,
      dequeue_key,
      processing_sorted_set_key,
      count,
      current_score
    ])
    |> do_bulk_dequeue()
  end

  def bulk_dequeue(
        dequeue_key,
        processing_sorted_set_key,
        limit_sorted_set_key,
        count,
        max_count,
        previous_score,
        current_score
      ) do
    Client.evalsha_command([
      @bulk_dequeue_limited_sha,
      _num_of_keys = 3,
      dequeue_key,
      processing_sorted_set_key,
      limit_sorted_set_key,
      count,
      max_count,
      previous_score,
      current_score
    ])
    |> do_bulk_dequeue()
  end

  def bulk_dequeue_optimistic(
        dequeue_key,
        processing_sorted_set_key,
        limit_sorted_set_key,
        count,
        max_count,
        previous_score,
        current_score
      ) do
    trim_rate_limiting_set!(limit_sorted_set_key, previous_score)

    jobs =
      rate_limited_fetch_jobs(
        dequeue_key,
        count,
        max_count,
        limit_sorted_set_key,
        previous_score,
        current_score
      )

    if Enum.empty?(jobs) do
      {:ok, []}
    else
      dequeue_jobs_limited!(
        jobs,
        dequeue_key,
        processing_sorted_set_key,
        limit_sorted_set_key,
        current_score
      )

      {:ok, jobs}
    end
  end

  defp rate_limited_fetch_jobs(
         dequeue_key,
         count,
         max_count,
         limit_sorted_set_key,
         previous_score,
         current_score
       ) do
    rate_limited_fetch_count(
      count,
      max_count,
      limit_sorted_set_key,
      previous_score,
      current_score
    )
    |> fetch_jobs(dequeue_key)
  end

  defp fetch_jobs(count, dequeue_key) when count > 0 do
    Client.lrange!(dequeue_key, 0, count - 1)
  end

  defp fetch_jobs(_count, _dequeue_key), do: []

  defp rate_limited_fetch_count(
         count,
         max_count,
         limit_sorted_set_key,
         previous_score,
         current_score
       ) do
    processed_count =
      rate_limit_processed_count(limit_sorted_set_key, previous_score, current_score)

    if processed_count < max_count do
      remaining_count = max_count - processed_count
      adjust_fetch_count(count, remaining_count)
    else
      0
    end
  end

  defp adjust_fetch_count(count, remaining_count) when remaining_count < count,
    do: remaining_count

  defp adjust_fetch_count(count, _remaining_count), do: count

  defp dequeue_jobs_limited!(
         jobs,
         dequeue_key,
         processing_sorted_set_key,
         limit_sorted_set_key,
         current_score
       ) do
    jobs_with_score = Enum.flat_map(jobs, fn job -> [current_score, job] end)

    ltrim_command = Client.ltrim_command(dequeue_key, length(jobs_with_score), -1)
    zadd_processing_command = Client.bulk_zadd_command(processing_sorted_set_key, jobs_with_score)
    lock_ttl = 60_000

    bulk_dequeue_lock_key(jobs)
    |> Client.cas!(lock_ttl, [zadd_processing_command, ltrim_command])

    Client.bulk_zadd_command(limit_sorted_set_key, jobs_with_score)
    |> Client.query!()
  end

  defp rate_limit_processed_count(key, previous_score, current_score) do
    processed_count_command = ["ZCOUNT", key, previous_score, current_score]
    Client.query!(processed_count_command)
  end

  defp trim_rate_limiting_set!(key, previous_score) do
    min_score = String.to_float(previous_score) - 1
    trim_rate_limit_command = ["ZREMRANGEBYSCORE", key, '-inf', min_score]
    Client.query!(trim_rate_limit_command)
  end

  defp bulk_dequeue_lock_key([job | _tail]) do
    "#{@bulk_dequeue_lock_prefix}:#{Jason.decode!(job)["jid"]}"
  end

  defp do_bulk_dequeue(command) do
    case Client.query(command) do
      {:error, reason} ->
        {:error, reason}

      {:ok, reponses} ->
        success_responses =
          reponses
          |> Enum.map(fn response ->
            case response do
              value when value in [:undefined, nil] ->
                nil

              error when error in [%Redix.Error{}, %Redix.ConnectionError{}] ->
                Logger.error("#{__MODULE__} - Error running command - #{Kernel.inspect(error)}")
                nil

              value ->
                value
            end
          end)
          |> Enum.reject(&is_nil/1)

        {:ok, success_responses}
    end
  end

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
    bulk_enqueue_commands(queues_and_jobs)
    |> Client.pipeline()
    |> case do
      {:error, reason} ->
        [{:error, reason}]

      {:ok, responses} ->
        queues_and_jobs
        |> Enum.zip(responses)
        |> Enum.map(fn {{scheduled_queue, _, job}, response} ->
          case response do
            value when value in [:undefined, nil] ->
              nil

            error when error in [%Redix.Error{}, %Redix.ConnectionError{}] ->
              Logger.error("Error running command - #{Kernel.inspect(error)}")
              nil

            _value ->
              {scheduled_queue, job}
          end
        end)
        |> Enum.reject(&is_nil/1)
    end
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

  defp bulk_enqueue_commands([]), do: []

  defp bulk_enqueue_commands([{_, queue_name, job} | queues_and_jobs]) do
    cmd = Client.rpush_command(queue_name, job)
    [cmd | bulk_enqueue_commands(queues_and_jobs)]
  end

  defp bulk_remove_scheduled_commands([]), do: []

  defp bulk_remove_scheduled_commands([{set_name, job} | scheduled_queues_and_jobs]) do
    cmd = Client.zrem_command(set_name, job)
    [cmd | bulk_remove_scheduled_commands(scheduled_queues_and_jobs)]
  end
end
