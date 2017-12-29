defmodule Flume.Support.JobSerializer do
  def encode!(job) do
    job |> serialized_job() |> Poison.encode!
  end

  def decode!(job) do
    job |> Poison.decode! |> deserialized_job()
  end

  def serialized_job(job) do
    %{
      args: Map.get(job, :args),
      class: Map.get(job, :class),
      enqueued_at: Map.get(job, :enqueued_at),
      error_message: Map.get(job, :error_message),
      error_class: Map.get(job, :error_class),
      failed_at: Map.get(job, :failed_at),
      finished_at: Map.get(job, :finished_at),
      jid: Map.get(job, :jid),
      processor: Map.get(job, :processor),
      queue: Map.get(job, :queue),
      retry_count: Map.get(job, :retry_count)
    }
  end

  defp deserialized_job(job) do
    %{
      args: Map.get(job, "args"),
      class: Map.get(job, "class"),
      enqueued_at: Map.get(job, "enqueued_at"),
      error_message: Map.get(job, "error_message"),
      error_class: Map.get(job, "error_class"),
      failed_at: Map.get(job, "failed_at"),
      finished_at: Map.get(job, "finished_at"),
      jid: Map.get(job, "jid"),
      processor: Map.get(job, "processor"),
      queue: Map.get(job, "queue"),
      retry_count: Map.get(job, "retry_count")
    }
  end
end
