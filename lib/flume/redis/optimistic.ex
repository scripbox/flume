defmodule Flume.Redis.Optimistic do
  alias Flume.Redis.Client
  alias Flume.Config

  @bulk_dequeue_lock_prefix "bulk_dequeue_lock"
  @dequeue_lock_ttl Config.dequeue_lock_ttl()

  def bulk_dequeue(dequeue_key, processing_sorted_set_key, count, current_score) do
    lrange(count, dequeue_key)
    |> dequeue_jobs!(
      dequeue_key,
      processing_sorted_set_key,
      current_score
    )
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
    trim_rate_limiting_set!(limit_sorted_set_key, previous_score)

    rate_limited_lrange(
      dequeue_key,
      count,
      max_count,
      limit_sorted_set_key,
      previous_score,
      current_score
    )
    |> dequeue_jobs_limited!(
      dequeue_key,
      processing_sorted_set_key,
      limit_sorted_set_key,
      current_score
    )
  end

  defp rate_limited_lrange(
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
    |> lrange(dequeue_key)
  end

  defp lrange(count, dequeue_key) when count > 0 do
    Client.lrange!(dequeue_key, 0, count - 1)
  end

  defp lrange(_count, _dequeue_key), do: []

  defp rate_limited_fetch_count(
         count,
         max_count,
         limit_sorted_set_key,
         previous_score,
         current_score
       ) do
    processed_count = Client.zcount!(limit_sorted_set_key, previous_score, current_score)

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
         [],
         _dequeue_key,
         _processing_sorted_set_key,
         _limit_sorted_set_key,
         _current_score
       ),
       do: {:ok, []}

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

    dequeued? =
      bulk_dequeue_lock_key(jobs)
      |> Client.cas!(@dequeue_lock_ttl, [zadd_processing_command, ltrim_command])

    if dequeued? do
      Client.bulk_zadd_command(limit_sorted_set_key, jobs_with_score)
      |> Client.query!()

      {:ok, jobs}
    else
      {:ok, []}
    end
  end

  defp dequeue_jobs!([], _dequeue_key, _processing_sorted_set_key, _current_score), do: {:ok, []}

  defp dequeue_jobs!(jobs, dequeue_key, processing_sorted_set_key, current_score) do
    jobs_with_score = Enum.flat_map(jobs, fn job -> [current_score, job] end)

    ltrim_command = Client.ltrim_command(dequeue_key, length(jobs_with_score), -1)
    zadd_processing_command = Client.bulk_zadd_command(processing_sorted_set_key, jobs_with_score)

    dequeued? =
      bulk_dequeue_lock_key(jobs)
      |> Client.cas!(@dequeue_lock_ttl, [zadd_processing_command, ltrim_command])

    if dequeued?, do: {:ok, jobs}, else: {:ok, []}
  end

  defp trim_rate_limiting_set!(key, previous_score) do
    Client.zremrangebyscore!(key, "-inf", previous_score)
  end

  defp bulk_dequeue_lock_key([job | _tail]) do
    "#{@bulk_dequeue_lock_prefix}:#{Jason.decode!(job)["jid"]}"
  end
end
