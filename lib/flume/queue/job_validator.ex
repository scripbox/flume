defmodule Flume.Queue.JobValidator do
  @moduledoc """
  Validates jobs before they are enqueued to prevent invalid jobs that are
  bound to fail from being processed.
  """

  alias Flume.Config

  @type validation_error ::
    {:invalid_queue, String.t()} |
    {:invalid_worker_module, atom()} |
    {:invalid_worker_function, {atom(), atom(), non_neg_integer()}}

  @doc """
  Validates a job before enqueueing.

  Checks:
  1. Queue name against configured pipelines
  2. Worker module existence
  3. Worker function existence (name + arity)

  Returns `:ok` if valid, `{:error, reason}` if invalid.
  """
  @spec validate_job(String.t() | atom(), atom(), atom(), list()) ::
    :ok | {:error, validation_error()}
  def validate_job(queue, worker_module, function_name, args) do
    with :ok <- validate_queue(queue),
         :ok <- validate_worker_module(worker_module),
         :ok <- validate_worker_function(worker_module, function_name, args) do
      :ok
    end
  end

  @doc """
  Validates multiple jobs for bulk enqueueing.

  Returns `:ok` if all jobs are valid, `{:error, {index, reason}}` if any job is invalid.
  """
  @spec validate_bulk_jobs(String.t() | atom(), list()) ::
    :ok | {:error, {non_neg_integer(), validation_error()}}
  def validate_bulk_jobs(queue, jobs) do
    jobs
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {job, index}, :ok ->
      case validate_single_bulk_job(queue, job) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {index, reason}}}
      end
    end)
  end

  # Private functions

  defp validate_single_bulk_job(queue, [worker_module, function_name, args]) do
    validate_job(queue, worker_module, function_name, args)
  end

  defp validate_single_bulk_job(queue, [worker_module, args]) do
    validate_job(queue, worker_module, :perform, args)
  end

  defp validate_single_bulk_job(_queue, _invalid_format) do
    {:error, {:invalid_job_format, "Job must be [worker, args] or [worker, function, args]"}}
  end

  defp validate_queue(queue) do
    queue_name = to_string(queue)
    configured_queues = Config.queues()

    if queue_name in configured_queues do
      :ok
    else
      {:error, {:invalid_queue, "Queue '#{queue_name}' is not configured in pipelines"}}
    end
  end

  defp validate_worker_module(worker_module) when is_atom(worker_module) do
    case Code.ensure_loaded(worker_module) do
      {:module, _} -> :ok
      {:error, _} -> {:error, {:invalid_worker_module, worker_module}}
    end
  end

  defp validate_worker_module(worker_module) do
    {:error, {:invalid_worker_module, worker_module}}
  end

  defp validate_worker_function(worker_module, function_name, args)
    when is_atom(function_name) and is_list(args) do
    arity = length(args)

    if function_exported?(worker_module, function_name, arity) do
      :ok
    else
      {:error, {:invalid_worker_function, {worker_module, function_name, arity}}}
    end
  end

  defp validate_worker_function(worker_module, function_name, args) do
    {:error, {:invalid_worker_function, {worker_module, function_name, length(args || [])}}}
  end
end
