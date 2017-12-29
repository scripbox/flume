defmodule Flume.Redis.Job do
  require Logger

  alias Flume.Support.Time
  alias Flume.Redis.Client

  def enqueue(redis_conn, queue_key, serialized_job) do
    try do
      response = Client.lpush(redis_conn, queue_key, serialized_job)

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

  def remove_job(redis_conn, queue_key, job) do
    Client.lrem!(redis_conn, queue_key, job)
  end

  def dequeue_bulk(redis_conn, dequeue_key, enqueue_key, count) do
    commands = Enum.map(1..count, fn(_) ->
      ["RPOPLPUSH", dequeue_key, enqueue_key]
    end)

    case Client.pipeline(redis_conn, commands) do
      {:error, reason} -> [{:error, reason}]
      {:ok, reponses} ->
        reponses |> Enum.map(fn(response) ->
          case response do
            :undefined -> {:ok, :none}
            nil -> {:ok, :none}
            %Redix.Error{} = error  -> {:error, error}
            value -> {:ok, value}
          end
        end)
    end
  end

  def schedule_job(redis_conn, queue_key, jid, job, schedule_at) do
    score = Time.time_to_score(schedule_at)
    try do
      case Client.zadd(redis_conn, queue_key, score, job) do
        {:ok, _} -> {:ok, jid}
        other    -> other
      end
    catch
      :exit, e ->
        Logger.info("Error enqueueing -  #{Kernel.inspect e}")
        {:error, :timeout}
    end
  end

  def fail_job(redis_conn, queue_key, job) do
    Client.zadd!(redis_conn, queue_key, Time.time_to_score, job)
  end
end
