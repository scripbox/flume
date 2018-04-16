defmodule Flume.Job.Scheduler do
  require Logger

  use GenServer

  alias Flume.Job.Manager

  defmodule State do
    defstruct namespace: nil, scheduler_poll_timeout: nil, job_name: nil
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: :"#{__MODULE__}_#{opts[:job_name]}")
  end

  def init(opts) do
    if  __MODULE__.__info__(:functions)[opts[:job_name]] do
      start_timeout(self())
      {:ok, struct(State, opts)}
    else
      {:stop, "Invalid job_name - #{opts[:job_name]}"}
    end
  end

  def start_timeout(pid), do: GenServer.cast(pid, :start_timeout)

  def handle_cast(:start_timeout, state) do
    handle_info(:timeout, state)
  end

  def handle_info(:timeout, state) do
    spawn_link(__MODULE__, state.job_name, [])

    {:noreply, state, state.scheduler_poll_timeout}
  end

  defdelegate retry_jobs, to: Manager, as: :retry_jobs

  defdelegate clear_completed_jobs, to: Manager, as: :clear_completed_jobs
end
