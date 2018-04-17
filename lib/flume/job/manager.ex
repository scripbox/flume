defmodule Flume.Job.Manager do
  use GenServer

  require Logger

  alias Flume.{Config, Event, Job}

  @ets_monitor_table_name :flume_job_manager_monitor
  @ets_monitor_options [
    :set,
    :public,
    :named_table,
    read_concurrency: false,
    write_concurrency: false
  ]
  @ets_enqueued_jobs_table_name :flume_enqueued_jobs
  @ets_enqueued_jobs_options [
    :bag,
    :public,
    :named_table,
    read_concurrency: false,
    write_concurrency: false
  ]
  @retry "retry"
  @completed "completed"
  @pool_name :"Flume.Job.Manager.pool"

  def pool_name, do: @pool_name

  def initialize_ets do
    :ets.new(@ets_monitor_table_name, @ets_monitor_options)
    :ets.new(@ets_enqueued_jobs_table_name, @ets_enqueued_jobs_options)
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def monitor(worker_pid, %Job{event: %Event{}} = job) do
    :poolboy.transaction(
      pool_name(),
      &GenServer.call(&1, {:monitor, worker_pid, job}),
      Config.get(:server_timeout)
    )
  end

  # Client API
  def init(opts) do
    {:ok, opts}
  end

  def handle_call(
        {:monitor, worker_pid, %Job{status: _status, event: %Event{}} = job},
        _from,
        state
      ) do
    ref = Process.monitor(worker_pid)
    store(worker_pid, ref, job)

    {:reply, ref, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, :normal}, state) do
    case find(pid) do
      [{^pid, _, job}] ->
        handle_down(job)
        :ets.delete(@ets_monitor_table_name, pid)

      _ ->
        :ok
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, msg}, state) do
    case find(pid) do
      [{^pid, _, job}] ->
        msg = if is_binary(msg) or is_atom(msg), do: msg, else: msg |> inspect()
        handle_down(%{job | error_message: msg})
        :ets.delete(@ets_monitor_table_name, pid)

      _ ->
        :ok
    end

    {:noreply, state}
  end

  # Helpers
  def retry_jobs do
    jobs = :ets.lookup(@ets_enqueued_jobs_table_name, @retry)

    jobs
    |> Enum.map(fn {"retry", %Job{event: event, error_message: error_message} = job} ->
      # TODO: Add a bulk retry function
      case Flume.retry_or_fail_job(event.queue, event.original_json, error_message) do
        {:ok, _} ->
          :ets.delete_object(@ets_enqueued_jobs_table_name, {@retry, job})

        _ ->
          nil
      end
    end)
  end

  def clear_completed_jobs do
    jobs =
      :ets.lookup(@ets_enqueued_jobs_table_name, @completed)
      |> Enum.map(&elem(&1, 1))

    with true <- Enum.any?(jobs),
         {:ok, _} <- Flume.remove_backup_jobs(jobs) do
      jobs |> Enum.map(&:ets.delete_object(@ets_enqueued_jobs_table_name, {@completed, &1}))
    else
      {:error, message} ->
        Logger.error("#{__MODULE__}: #{message}")

      _ ->
        nil
    end
  end

  defp store(pid, ref, %Job{status: _status, event: _event} = job) do
    :ets.insert(
      @ets_monitor_table_name,
      {pid, ref, job}
    )
  end

  defp find(pid) do
    :ets.lookup(@ets_monitor_table_name, pid)
  end

  defp handle_down(%Job{status: :started} = job) do
    :ets.insert(@ets_enqueued_jobs_table_name, {@retry, job})
  end

  defp handle_down(%Job{status: :processed, event: event}) do
    :ets.insert(@ets_enqueued_jobs_table_name, {@completed, event |> Poison.encode!()})
  end
end
