defmodule Flume.Queue.Scheduler do
  require Logger

  use GenServer

  alias Flume.Queue.Manager
  alias Flume.Support.Time

  defmodule State do
    defstruct namespace: nil, scheduler_poll_timeout: nil, poll_timeout: nil
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
    spawn_link(__MODULE__, :work, [state])

    {:noreply, state, state.scheduler_poll_timeout}
  end

  def work(state) do
    response = Manager.remove_and_enqueue_scheduled_jobs(
      state.namespace,
      Time.time_to_score
    )
    case response do
      {:ok, 0} ->
        Logger.info("#{__MODULE__}: Waiting for new jobs")
      {:ok, count} ->
        Logger.info("#{__MODULE__}: Processed #{count} jobs")
    end
  end
end
