defmodule Flume.Pipeline.SystemEvent.Worker do
  @moduledoc """
  A worker will spawn a task for each event.
  """
  use Retry

  alias Flume.{Config, Event}
  alias Flume.Queue.Manager, as: QueueManager
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
    retry with: exponential_backoff() |> randomize() |> expiry(@retry_expiry_timeout) do
      QueueManager.remove_processing(Config.namespace(), event.queue, event.original_json)
    after
      result -> result
    else
      _ -> SystemEvent.Producer.enqueue({:success, event})
    end
  end

  def fail(event, error_message) do
    retry with: exponential_backoff() |> randomize() |> expiry(@retry_expiry_timeout) do
      QueueManager.retry_or_fail_job(
        Config.namespace(),
        event.queue,
        event.original_json,
        error_message
      )
    after
      result -> result
    else
      _ -> SystemEvent.Producer.enqueue({:failed, event, error_message})
    end
  end
end
