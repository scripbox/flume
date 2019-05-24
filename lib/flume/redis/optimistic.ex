defmodule Flume.Redis.Optimistic do
  alias Flume.Redis.Client
  alias Flume.Config

  @bulk_dequeue_lock_prefix "bulk_dequeue_lock"
  @dequeue_lock_ttl Config.dequeue_lock_ttl()
  @dequeue_retry_count Config.dequeue_retry_count()

  def bulk_dequeue(
        dequeue_key,
        processing_sorted_set_key,
        count,
        current_score
      ),
      do:
        do_bulk_dequeue(
          dequeue_key,
          processing_sorted_set_key,
          count,
          current_score,
          @dequeue_retry_count
        )

  def do_bulk_dequeue(
        _dequeue_key,
        _processing_sorted_set_key,
        _count,
        _current_score,
        0
      ),
      do: {:error, :locked}

  def do_bulk_dequeue(
        dequeue_key,
        processing_sorted_set_key,
        count,
        current_score,
        retry_count
      ) do
    dequeue_status =
      lrange(count, dequeue_key)
      |> dequeue!(
        dequeue_key,
        processing_sorted_set_key,
        current_score
      )

    case dequeue_status do
      {:ok, jobs} ->
        {:ok, jobs}

      {:error, :locked} ->
        do_bulk_dequeue(
          dequeue_key,
          processing_sorted_set_key,
          count,
          current_score,
          retry_count - 1
        )
    end
  end

  def bulk_dequeue_rate_limited(
        dequeue_key,
        processing_sorted_set_key,
        limit_sorted_set_key,
        count,
        max_count,
        previous_score,
        current_score
      ),
      do:
        do_bulk_dequeue_rate_limited(
          dequeue_key,
          processing_sorted_set_key,
          limit_sorted_set_key,
          count,
          max_count,
          previous_score,
          current_score,
          @dequeue_retry_count
        )

  def do_bulk_dequeue_rate_limited(
        _dequeue_key,
        _processing_sorted_set_key,
        _limit_sorted_set_key,
        _count,
        _max_count,
        _previous_score,
        _current_score,
        0
      ),
      do: {:error, :locked}

  def do_bulk_dequeue_rate_limited(
        dequeue_key,
        processing_sorted_set_key,
        limit_sorted_set_key,
        count,
        max_count,
        previous_score,
        current_score,
        retry_count
      ) do
    clean_up_rate_limiting(limit_sorted_set_key, previous_score)

    dequeue_status =
      rate_limited_lrange(
        dequeue_key,
        count,
        max_count,
        limit_sorted_set_key,
        previous_score,
        current_score
      )
      |> dequeue_limited!(
        dequeue_key,
        processing_sorted_set_key,
        limit_sorted_set_key,
        current_score
      )

    case dequeue_status do
      {:ok, jobs} ->
        {:ok, jobs}

      {:error, :locked} ->
        do_bulk_dequeue_rate_limited(
          dequeue_key,
          processing_sorted_set_key,
          limit_sorted_set_key,
          count,
          max_count,
          previous_score,
          current_score,
          retry_count - 1
        )
    end
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

  defp dequeue_limited!(
         [],
         _dequeue_key,
         _processing_sorted_set_key,
         _limit_sorted_set_key,
         _current_score
       ),
       do: {:ok, []}

  defp dequeue_limited!(
         jobs,
         dequeue_key,
         processing_sorted_set_key,
         limit_sorted_set_key,
         current_score
       ) do
    jobs_with_score = Enum.flat_map(jobs, fn job -> [current_score, job] end)

    ltrim_command = Client.ltrim_command(dequeue_key, length(jobs), -1)
    zadd_processing_command = Client.bulk_zadd_command(processing_sorted_set_key, jobs_with_score)

    dequeue_status =
      bulk_dequeue_lock_key(jobs)
      |> Client.cas!(@dequeue_lock_ttl, [zadd_processing_command, ltrim_command])

    case dequeue_status do
      :locked ->
        {:error, :locked}

      :ok ->
        Client.bulk_zadd_command(limit_sorted_set_key, jobs_with_score)
        |> Client.query!()

        {:ok, jobs}
    end
  end

  defp dequeue!([], _dequeue_key, _processing_sorted_set_key, _current_score), do: {:ok, []}

  defp dequeue!(jobs, dequeue_key, processing_sorted_set_key, current_score) do
    jobs_with_score = Enum.flat_map(jobs, fn job -> [current_score, job] end)

    ltrim_command = Client.ltrim_command(dequeue_key, length(jobs_with_score), -1)
    zadd_processing_command = Client.bulk_zadd_command(processing_sorted_set_key, jobs_with_score)

    dequeue_status =
      bulk_dequeue_lock_key(jobs)
      |> Client.cas!(@dequeue_lock_ttl, [zadd_processing_command, ltrim_command])

    case dequeue_status do
      :locked -> {:error, :locked}
      :ok -> {:ok, jobs}
    end
  end

  defp clean_up_rate_limiting(key, previous_score) do
    Client.zremrangebyscore!(key, "-inf", previous_score)
  end

  defp bulk_dequeue_lock_key([job | _tail]) do
    "#{@bulk_dequeue_lock_prefix}:#{Jason.decode!(job)["jid"]}"
  end
end
