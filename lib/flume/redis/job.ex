defmodule Flume.Redis.Job do
  require Logger

  alias Flume.Support.Time
  alias Flume.Redis.Client

  def enqueue(redis_conn, queue_key, job) do
    try do
      response = Client.lpush(redis_conn, queue_key, job)

      case response do
        {:ok, [%Redix.Error{}, %Redix.Error{}]} = error -> error
        {:ok, [%Redix.Error{}, _]} = error -> error
        {:ok, [_, %Redix.Error{}]} = error -> error
        {:ok, [_, _]} -> :ok
        other -> other
      end
    catch
      :exit, e ->
        Logger.info("Error enqueueing -  #{Kernel.inspect e}")
        {:error, :timeout}
    end
  end

  def bulk_dequeue(redis_conn, dequeue_key, enqueue_key, count) do
    commands = Enum.map(1..count, fn(_) ->
      ["RPOPLPUSH", dequeue_key, enqueue_key]
    end)

    case Client.pipeline(redis_conn, commands) do
      {:error, reason} -> [{:error, reason}]
      {:ok, reponses} ->
        reponses |> Enum.map(fn(response) ->
          case response do
            value when value in [:undefined, nil]  -> nil
            error when error in [%Redix.Error{}, %Redix.ConnectionError{}] ->
              Logger.error("Error running command - #{Kernel.inspect error}")
              nil
            value -> value
          end
        end)
    end |> Enum.reject(&is_nil/1)
  end

  def bulk_enqueue_scheduled!(redis_conn, queues_and_jobs) do
    commands = bulk_enqueue_commands(queues_and_jobs)

    case Client.pipeline(redis_conn, commands) do
      {:error, reason} -> [{:error, reason}]
      {:ok, responses} ->
        queues_and_jobs
        |> Enum.zip(responses)
        |> Enum.map(fn({{scheduled_queue, _, job}, response}) ->
          case response do
            value when value in [:undefined, nil]  -> nil
            error when error in [%Redix.Error{}, %Redix.ConnectionError{}] ->
              Logger.error("Error running command - #{Kernel.inspect error}")
              nil
            _value -> {scheduled_queue, job}
          end
        end)
        |> Enum.reject(&is_nil/1)
    end
  end

  def bulk_remove_scheduled!(redis_conn, scheduled_queues_and_jobs) do
    commands = bulk_remove_scheduled_commands(scheduled_queues_and_jobs)

    case Client.pipeline(redis_conn, commands) do
      {:error, reason} -> [{:error, reason}]
      {:ok, responses} ->
        scheduled_queues_and_jobs
        |> Enum.zip(responses)
        |> Enum.map(fn({{_, job}, response}) ->
          case response do
            value when value in [:undefined, nil]  -> nil
            error when error in [%Redix.Error{}, %Redix.ConnectionError{}] ->
              Logger.error("Error running command - #{Kernel.inspect error}")
              nil
            _value -> job
          end
        end)
        |> Enum.reject(&is_nil/1)
    end
  end

  def schedule_job(redis_conn, queue_key, schedule_at, job) do
    score = Time.time_to_score(schedule_at)
    try do
      case Client.zadd(redis_conn, queue_key, score, job) do
        {:ok, jid} -> {:ok, jid}
        other -> other
      end
    catch
      :exit, e ->
        Logger.info("Error enqueueing -  #{Kernel.inspect e}")
        {:error, :timeout}
    end
  end

  def remove_job!(redis_conn, queue_key, job) do
    Client.lrem!(redis_conn, queue_key, job)
  end

  def remove_scheduled_job!(redis_conn, queue_key, job) do
    Client.zrem!(redis_conn, queue_key, job)
  end

  def fail_job!(redis_conn, queue_key, job) do
    Client.zadd!(redis_conn, queue_key, Time.time_to_score, job)
  end

  def fetch_all!(redis_conn, queue_key) do
    Client.lrange!(redis_conn, queue_key)
  end

  def fetch_all!(redis_conn, :retry, queue_key) do
    Client.zrange!(redis_conn, queue_key)
  end

  def scheduled_jobs(redis_conn, queues, score) do
    commands = Enum.map(queues, &(["ZRANGEBYSCORE", &1, 0, score]))

    case Client.pipeline(redis_conn, commands) do
      {:error, reason} -> [{:error, reason}]
      {:ok, response} ->
        updated_jobs =
          response
          |> Enum.map(fn(jobs) ->
            Enum.map(jobs, fn(job) ->
              case job do
                value when value in [:undefined, nil]  -> nil
                error when error in [%Redix.Error{}, %Redix.ConnectionError{}] ->
                  Logger.error("Error running command - #{Kernel.inspect error}")
                  nil
                value -> value
              end
            end)
            |> Enum.reject(&is_nil/1)
          end)
        Enum.zip(queues, updated_jobs)
    end
  end

  defp bulk_enqueue_commands([]), do: []
  defp bulk_enqueue_commands([{_, queue_name, job}|queues_and_jobs]) do
    cmd = ["LPUSH", queue_name, job]
    [cmd | bulk_enqueue_commands(queues_and_jobs)]
  end

  defp bulk_remove_scheduled_commands([]), do: []
  defp bulk_remove_scheduled_commands([{set_name, job}|scheduled_queues_and_jobs]) do
    cmd = ["ZREM", set_name, job]
    [cmd | bulk_remove_scheduled_commands(scheduled_queues_and_jobs)]
  end
end
