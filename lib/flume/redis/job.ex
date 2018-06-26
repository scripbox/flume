defmodule Flume.Redis.Job do
  require Logger

  alias Flume.Support.Time
  alias Flume.Redis.Client

  def enqueue(queue_key, job) do
    try do
      response = Client.lpush(queue_key, job)

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
    commands = Enum.map(jobs, fn job ->
      Client.lpush_command(queue_key, job)
    end)

    case Client.pipeline(commands) do
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

  def bulk_dequeue(dequeue_key, enqueue_key, count) do
    commands =
      Enum.map(1..count, fn _ ->
        ["RPOPLPUSH", dequeue_key, enqueue_key]
      end)

    case Client.pipeline(commands) do
      {:error, reason} ->
        [{:error, reason}]

      {:ok, reponses} ->
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
    end
  end

  def bulk_enqueue_scheduled!(queues_and_jobs) do
    commands = bulk_enqueue_commands(queues_and_jobs)

    case Client.pipeline(commands) do
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
    commands = bulk_remove_scheduled_commands(scheduled_queues_and_jobs)

    case Client.pipeline(commands) do
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
      case Client.zadd(queue_key, score, job) do
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

  def remove_scheduled_job!(queue_key, job) do
    Client.zrem!(queue_key, job)
  end

  def fail_job!(queue_key, job) do
    Client.zadd!(queue_key, Time.time_to_score(), job)
  end

  def fetch_all!(queue_key) do
    Client.lrange!(queue_key)
  end

  def fetch_all!(:retry, queue_key) do
    Client.zrange!(queue_key)
  end

  def scheduled_jobs(queues, score) do
    commands = Enum.map(queues, &["ZRANGEBYSCORE", &1, 0, score])

    case Client.pipeline(commands) do
      {:error, reason} ->
        {:error, reason}

      {:ok, response} ->
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
    cmd = ["LPUSH", queue_name, job]
    [cmd | bulk_enqueue_commands(queues_and_jobs)]
  end

  defp bulk_remove_scheduled_commands([]), do: []

  defp bulk_remove_scheduled_commands([{set_name, job} | scheduled_queues_and_jobs]) do
    cmd = ["ZREM", set_name, job]
    [cmd | bulk_remove_scheduled_commands(scheduled_queues_and_jobs)]
  end
end
