defmodule Flume.Pipeline.SystemEvent.Worker do
  @moduledoc """
  A worker will spawn a task for each event.
  """
  use Retry

  alias Flume.Event
  alias Flume.Pipeline.SystemEvent

  # In milliseconds
  @retry_expiry_timeout 10_000

  def start_link({:success, %Event{} = event}) do
    Task.start_link(__MODULE__, :success, [event])
  end

  def start_link({:failed, %Event{} = event, exception_message}) do
    Task.start_link(__MODULE__, :fail, [event, exception_message])
  end

  def success(event) do
    retry with: exp_backoff() |> randomize() |> expiry(@retry_expiry_timeout) do
      Flume.remove_backup(event.queue, event.original_json)
    end
    |> case do
      {:error, _} ->
        SystemEvent.Producer.enqueue({:success, event})

      _ ->
        nil
    end
  end

  def fail(event, error_message) do
    retry with: exp_backoff() |> randomize() |> expiry(@retry_expiry_timeout) do
      Flume.retry_or_fail_job(
        event.queue,
        event.original_json,
        error_message
      )
    end
    |> case do
      {:error, _} ->
        SystemEvent.Producer.enqueue({:failed, event, error_message})

      _ ->
        nil
    end
  end
end
