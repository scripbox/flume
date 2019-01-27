defmodule Flume.Queue.Scheduler do
  require Flume.Logger

  use GenServer

  alias Flume.Logger
  alias Flume.Queue.Manager
  alias Flume.Support.Time

  defmodule State do
    defstruct namespace: nil, scheduler_poll_interval: nil
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts \\ []) do
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
    Manager.remove_and_enqueue_scheduled_jobs(
      state.namespace,
      Time.time_to_score()
    )
    |> case do
      {:ok, 0} ->
        Logger.debug("#{__MODULE__}: Waiting for new jobs")

      {:ok, count} ->
        Logger.debug("#{__MODULE__}: Processed #{count} jobs")

      {:error, error_message} ->
        Logger.error("#{__MODULE__}: Failed to fetch jobs - #{Kernel.inspect(error_message)}")
    end
  end

  defp schedule_work(state) do
    Process.send_after(self(), :schedule_work, state.scheduler_poll_interval)
  end
end
