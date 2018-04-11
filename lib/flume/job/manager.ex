defmodule Flume.Job.Manager do
  use GenServer

  require Logger

  alias Flume.{Event, Job}

  @ets_monitor_table_name :flume_job_manager_monitor
  @ets_monitor_options [:set, :public, :named_table, read_concurrency: true]
  @ets_enqueued_jobs_table_name :flume_enqueued_jobs
  @ets_enqueued_jobs_options [:bag, :public, :named_table, read_concurrency: true]
  @retry "retry"
  @completed "completed"

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def failed(worker_pid, error_message) do
    GenServer.cast(__MODULE__, {:failed, worker_pid, error_message})
  end

  def monitor(worker_pid, %Job{event: %Event{}} = job) do
    GenServer.cast(__MODULE__, {:monitor, worker_pid, job})
  end

  def unmonitor(worker_pid) do
    GenServer.cast(__MODULE__, {:unmonitor, worker_pid})
  end

  # Client API
  def init(opts) do
    :ets.new(@ets_monitor_table_name, @ets_monitor_options)
    :ets.new(@ets_enqueued_jobs_table_name, @ets_enqueued_jobs_options)

    {:ok, opts}
  end

  def handle_cast({:failed, worker_pid, error_message}, state) do
    [{^worker_pid, _, job}] = find(worker_pid)
    handle_down(%{job | error_message: error_message})

    {:noreply, state}
  end

  def handle_cast({:monitor, worker_pid, %Job{status: _status, event: %Event{}} = job}, state) do
    ref = Process.monitor(worker_pid)
    store(worker_pid, ref, job)

    {:noreply, state}
  end

  def handle_cast({:unmonitor, worker_pid}, state) do
    [{^worker_pid, ref, _job}] = find(worker_pid)
    Process.demonitor(ref)

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, :normal}, state) do
    case find(pid) do
      [{^pid, _, job}] ->
        handle_down(job)

      _ ->
        :ok
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, msg}, state) do
    case find(pid) do
      [{^pid, _, job}] ->
        handle_down(%{job | error_message: msg})

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
      case Flume.retry_or_fail_job(event.queue, event |> Poison.encode(), error_message) do
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
