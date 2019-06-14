defmodule Flume.Redis.BulkDequeue do
  require Flume.Logger

  alias Flume.Redis.{Client, Lock}
  alias Flume.{Config, Utils, Logger}

  @dequeue_lock_suffix "bulk_dequeue_lock"
  @dequeue_lock_ttl Config.dequeue_lock_ttl()
  # This should always be much lower than dequeue_lock_ttl
  @dequeue_process_timeout Config.dequeue_process_timeout()

  def exec(
        dequeue_key,
        processing_sorted_set_key,
        count,
        current_score
      ) do
    lock_key = bulk_dequeue_lock_key(dequeue_key)

    case acquire_lock(lock_key) do
      {:ok, lock_token} ->
        exec(
          dequeue_key,
          processing_sorted_set_key,
          count,
          current_score,
          lock_token
        )

      {:error, error} ->
        Logger.debug("[Dequeue] #{dequeue_key}:#{count} error: #{inspect(error)}")
        {:ok, []}
    end
  end

  def exec(
        dequeue_key,
        processing_sorted_set_key,
        count,
        current_score,
        lock_token
      ) do
    result =
      Utils.safe_apply(
        fn ->
          lrange(count, dequeue_key)
          |> dequeue(
            dequeue_key,
            processing_sorted_set_key,
            current_score
          )
        end,
        @dequeue_process_timeout
      )

    case bulk_dequeue_lock_key(dequeue_key)
         |> release_lock(lock_token) do
      {:error, reason} ->
        Logger.error("[Lock release] #{dequeue_key}: error: #{inspect(reason)}")

      :ok ->
        :ok
    end

    case result do
      {:ok, result} ->
        result

      {:exit, {error, _stack}} ->
        Logger.error("[Dequeue] #{dequeue_key}:#{count} error: #{inspect(error)}")
        {:ok, []}

      {:timeout, reason} ->
        Logger.error("[Dequeue] #{dequeue_key}:#{count} error: #{inspect(reason)}")
        {:ok, []}
    end
  end

  def exec_rate_limited(
        dequeue_key,
        processing_sorted_set_key,
        limit_sorted_set_key,
        count,
        max_count,
        previous_score,
        current_score
      ) do
    lock_key = bulk_dequeue_lock_key(dequeue_key)

    case acquire_lock(lock_key) do
      {:ok, lock_token} ->
        exec_rate_limited(
          dequeue_key,
          processing_sorted_set_key,
          limit_sorted_set_key,
          count,
          max_count,
          previous_score,
          current_score,
          lock_token
        )

      {:error, error} ->
        Logger.debug("[Dequeue] #{dequeue_key}:#{count} error: #{inspect(error)}")
        {:ok, []}
    end
  end

  def exec_rate_limited(
        dequeue_key,
        processing_sorted_set_key,
        limit_sorted_set_key,
        count,
        max_count,
        previous_score,
        current_score,
        lock_token
      ) do
    result =
      Utils.safe_apply(
        fn ->
          clean_up_rate_limiting(limit_sorted_set_key, previous_score)

          lrange(
            dequeue_key,
            count,
            max_count,
            limit_sorted_set_key,
            previous_score,
            current_score
          )
          |> dequeue(
            dequeue_key,
            processing_sorted_set_key,
            limit_sorted_set_key,
            current_score
          )
        end,
        @dequeue_process_timeout
      )

    case bulk_dequeue_lock_key(dequeue_key)
         |> release_lock(lock_token) do
      {:error, reason} ->
        Logger.error("[Lock release] #{dequeue_key}: error: #{inspect(reason)}")

      :ok ->
        :ok
    end

    case result do
      {:ok, result} ->
        result

      {:exit, reason} ->
        Logger.error("[Dequeue] #{dequeue_key}:#{count} error: #{inspect(reason)}")
        {:ok, []}

      {:timeout, reason} ->
        Logger.error("[Dequeue] #{dequeue_key}:#{count} error: #{inspect(reason)}")
        {:ok, []}
    end
  end

  defp lrange(
         dequeue_key,
         count,
         max_count,
         limit_sorted_set_key,
         previous_score,
         current_score
       ) do
    fetch_count(
      count,
      max_count,
      limit_sorted_set_key,
      previous_score,
      current_score
    )
    |> lrange(dequeue_key)
  end

  defp lrange(count, dequeue_key) when count > 0 do
    case Client.lrange(dequeue_key, 0, count - 1) do
      {:ok, res} ->
        res

      {:error, reason} ->
        Logger.error("[Dequeue] #{dequeue_key}:#{count} error: #{inspect(reason)}")
        []
    end
  end

  defp lrange(_count, _dequeue_key), do: []

  defp fetch_count(
         count,
         max_count,
         limit_sorted_set_key,
         previous_score,
         _current_score
       ) do
    processed_count =
      case Client.zcount(limit_sorted_set_key, previous_score) do
        {:ok, count} ->
          count

        {:error, reason} ->
          Logger.error("[Dequeue] #{limit_sorted_set_key}:#{count} error: #{inspect(reason)}")
          :infinity
      end

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

  defp dequeue(
         [],
         _dequeue_key,
         _processing_sorted_set_key,
         _limit_sorted_set_key,
         _current_score
       ),
       do: {:ok, []}

  defp dequeue(
         jobs,
         dequeue_key,
         processing_sorted_set_key,
         limit_sorted_set_key,
         current_score
       ) do
    jobs_with_score = Enum.flat_map(jobs, fn job -> [current_score, job] end)
    trimmed_jobs_with_score = Enum.flat_map(jobs, fn job -> [current_score, checksum(job)] end)

    ltrim_command = Client.ltrim_command(dequeue_key, length(jobs), -1)
    zadd_processing_command = Client.bulk_zadd_command(processing_sorted_set_key, jobs_with_score)
    zadd_limit_command = Client.bulk_zadd_command(limit_sorted_set_key, trimmed_jobs_with_score)

    job_length = length(jobs)
    expected_response = {:ok, [job_length, "OK", job_length]}

    case Client.transaction_pipeline([
           zadd_processing_command,
           ltrim_command,
           zadd_limit_command
         ]) do
      ^expected_response ->
        {:ok, jobs}

      error_res ->
        Logger.info(
          "[Dequeue]: #{dequeue_key} error: Expected: #{inspect(expected_response)}, Got: #{
            inspect(error_res)
          }, queue_name: #{dequeue_key}"
        )

        {:ok, []}
    end
  end

  defp checksum(job), do: :crypto.hash(:md5, job) |> Base.encode16()

  defp dequeue([], _dequeue_key, _processing_sorted_set_key, _current_score), do: {:ok, []}

  defp dequeue(jobs, dequeue_key, processing_sorted_set_key, current_score) do
    jobs_with_score = Enum.flat_map(jobs, fn job -> [current_score, job] end)

    ltrim_command = Client.ltrim_command(dequeue_key, length(jobs), -1)
    zadd_processing_command = Client.bulk_zadd_command(processing_sorted_set_key, jobs_with_score)

    expected_response = {:ok, [length(jobs), "OK"]}

    case Client.transaction_pipeline([
           zadd_processing_command,
           ltrim_command
         ]) do
      ^expected_response ->
        {:ok, jobs}

      error_res ->
        Logger.info(
          "[Dequeue]: error: Expected: #{inspect(expected_response)}, Got: #{inspect(error_res)}, queue_name: #{
            dequeue_key
          }"
        )

        {:ok, []}
    end
  end

  defp clean_up_rate_limiting(key, previous_score) do
    Client.zremrangebyscore(key, "-inf", previous_score)
  end

  defp acquire_lock(lock_key),
    do: Lock.acquire(lock_key, @dequeue_lock_ttl)

  defdelegate release_lock(lock_key, token), to: Lock, as: :release

  defp bulk_dequeue_lock_key(dequeue_key) do
    "#{dequeue_key}:#{@dequeue_lock_suffix}"
  end
end
