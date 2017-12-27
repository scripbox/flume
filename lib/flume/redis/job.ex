defmodule Flume.Redis.Job do
  require Logger

  alias Flume.Redis.Client

  def enqueue(redis_conn, namespace, queue, serialized_job) do
    try do
      response = Client.lpush(redis_conn, queue_key(namespace, queue), serialized_job)

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

  def dequeue(redis_conn, namespace, queue, job) do
    Client.lrem!(redis_conn, queue_key(namespace, queue), job)
  end

  def dequeue_bulk(redis_conn, namespace, queue, count) do
    commands = Enum.map(1..count, fn(_) ->
      ["RPOPLPUSH", queue_key(namespace, queue), backup_key(namespace, queue)]
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

  defp queue_key(namespace, queue) do
    "#{namespace}:#{queue}"
  end

  defp backup_key(namespace, queue) do
    "#{namespace}:backup:#{queue}"
  end
end
