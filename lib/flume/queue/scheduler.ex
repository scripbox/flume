defmodule Flume.Queue.Scheduler do
  require Logger

  use GenServer

  alias Flume.Queue.Manager
  alias Flume.Support.Time

  @external_resource "priv/scripts/rpop_lpush_zadd.lua"
  @external_resource "priv/scripts/enqueue_backup_jobs.lua"

  # 15 minutes
  @default_visibility_timeout 900

  defmodule State do
    defstruct namespace: nil, scheduler_poll_timeout: nil, poll_timeout: nil
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts \\ []) do
    Flume.Redis.Script.load()
    state = struct(State, opts)
    schedule_work(state)

    {:ok, state}
  end

  def handle_info(:schedule_work, state) do
    work(state)
    schedule_work(state)

    {:noreply, state}
  end

  defp work(state) do
    remove_and_enqueue_scheduled_jobs(state)
    enqueue_backup_jobs(state)
  end

  defp remove_and_enqueue_scheduled_jobs(state) do
    response =
      Manager.remove_and_enqueue_scheduled_jobs(
        state.namespace,
        Time.time_to_score()
      )

    case response do
      {:ok, 0} ->
        Logger.debug("#{__MODULE__}: Waiting for new jobs")

      {:ok, count} ->
        Logger.debug("#{__MODULE__}: Processed #{count} jobs")

      {:error, error_message} ->
        Logger.error("#{__MODULE__}: Failed to fetch jobs - #{Kernel.inspect(error_message)}")
    end
  end

  defp enqueue_backup_jobs(state) do
    utc_time =
      DateTime.utc_now()
      |> DateTime.to_unix()
      |> Kernel.-(@default_visibility_timeout)
      |> DateTime.from_unix!()

    response = Manager.enqueue_backup_jobs(state.namespace, utc_time)

    case response do
      {:ok, 0} ->
        Logger.debug("#{__MODULE__}: No backup jobs to enqueue")

      {:ok, count} ->
        Logger.debug("#{__MODULE__}: Enqueue #{count} jobs from backup queue")

      {:error, error_message} ->
        Logger.error("#{__MODULE__}: Failed to enqueue jobs - #{Kernel.inspect(error_message)}")
    end
  end

  defp schedule_work(state) do
    Process.send_after(self(), :schedule_work, state.scheduler_poll_timeout)
  end
end
