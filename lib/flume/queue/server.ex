defmodule Flume.Queue.Server do
  alias Flume.Queue.Manager

  defmodule State do
    defstruct namespace: nil, poll_timeout: nil
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def enqueue(pid, queue, worker, function_name, args) do
    GenServer.call(pid, {:enqueue, queue, worker, function_name, args})
  end

  def enqueue_in(pid, queue, time_in_seconds, worker, function_name, args) do
    GenServer.call(pid, {:enqueue_in, queue, time_in_seconds, worker, function_name, args})
  end

  def fetch_jobs(pid, queue, count) do
    GenServer.call(pid, {:fetch_jobs, queue, count})
  end

  def retry_or_fail_job(pid, queue, job, error) do
    GenServer.call(pid, {:retry_or_fail_job, queue, job, error})
  end

  def remove_job(pid, queue, job) do
    GenServer.call(pid, {:remove_job, queue, job})
  end

  def remove_retry(pid, job) do
    GenServer.call(pid, {:remove_retry, job})
  end

  def remove_backup(pid, queue, job) do
    GenServer.call(pid, {:remove_backup, queue, job})
  end

  def init(opts) do
    {:ok, struct(State, opts)}
  end

  def handle_call({:enqueue, queue, worker, function_name, args}, _from, state) do
    response = Manager.enqueue(state.namespace, queue, worker, function_name, args)
    {:reply, response, state}
  end

  def handle_call(
        {:enqueue_in, queue, time_in_seconds, worker, function_name, args},
        _from,
        state
      ) do
    response =
      Manager.enqueue_in(state.namespace, queue, time_in_seconds, worker, function_name, args)

    {:reply, response, state}
  end

  def handle_call({:fetch_jobs, queue, count}, _from, state) do
    response = Manager.fetch_jobs(state.namespace, queue, count)
    {:reply, response, state}
  end

  def handle_call({:retry_or_fail_job, queue, job, error}, _from, state) do
    response = Manager.retry_or_fail_job(state.namespace, queue, job, error)
    {:reply, response, state}
  end

  def handle_call({:remove_job, queue, job}, _from, state) do
    response = Manager.remove_job(state.namespace, queue, job)
    {:reply, response, state}
  end

  def handle_call({:remove_retry, job}, _from, state) do
    response = Manager.remove_retry(state.namespace, job)
    {:reply, response, state}
  end

  def handle_call({:remove_backup, queue, job}, _from, state) do
    response = Manager.remove_backup(state.namespace, queue, job)
    {:reply, response, state}
  end
end
