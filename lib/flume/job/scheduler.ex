defmodule Flume.Job.Scheduler do
  require Logger

  use GenServer

  alias Flume.Job.Manager

  defmodule State do
    defstruct namespace: nil, scheduler_poll_timeout: nil
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts \\ []) do
    start_timeout(self())

    {:ok, struct(State, opts)}
  end

  def start_timeout(pid), do: GenServer.cast(pid, :start_timeout)

  def handle_cast(:start_timeout, state) do
    handle_info(:timeout, state)
  end

  def handle_info(:timeout, state) do
    spawn_link(__MODULE__, :work, [])

    {:noreply, state, state.scheduler_poll_timeout}
  end

  def work do
    Manager.retry_jobs()
    Manager.clear_completed_jobs()
  end
end
