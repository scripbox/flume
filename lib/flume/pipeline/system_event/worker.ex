defmodule Flume.Pipeline.SystemEvent.Worker do
  @moduledoc """
  A worker will spawn a task for each event.
  """

  require Flume.Utils.Retry

  alias Flume.{Event, Utils.Retry}

  def start_link({:success, %Event{} = event}) do
    Task.start_link(__MODULE__, :success, [event])
  end

  def start_link({:failed, %Event{} = event, exception_message}) do
    Task.start_link(__MODULE__, :fail, [event, exception_message])
  end

  def success(event) do
    Retry.retry do: Flume.remove_backup(event.queue, event.original_json)
  end

  def fail(event, exception_message) do
    Retry.retry do
      Flume.retry_or_fail_job(
        event.queue,
        event.original_json,
        exception_message
      )
    end
  end
end
