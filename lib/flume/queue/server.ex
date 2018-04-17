defmodule Flume.Queue.Server do
  require Logger

  use GenServer

  alias Flume.Queue.Manager

  @pool_name :"Flume.Queue.Server.pool"

  defmodule State do
    defstruct namespace: nil, poll_timeout: nil
  end

  def pool_name, do: @pool_name

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def enqueue(pid, queue, worker, function_name, args) do
    GenServer.call(pid, {:enqueue, queue, worker, function_name, args})
  catch
    :exit, {:timeout, _} -> :timeout
  end

  def enqueue_in(pid, queue, time_in_seconds, worker, function_name, args) do
    GenServer.call(pid, {:enqueue_in, queue, time_in_seconds, worker, function_name, args})
  catch
    :exit, {:timeout, _} -> :timeout
  end

  def fetch_jobs(pid, queue, count) do
    GenServer.call(pid, {:fetch_jobs, queue, count})
  catch
    :exit, {:timeout, _} -> :timeout
  end

  def retry_or_fail_job(pid, queue, job, error) do
    GenServer.call(pid, {:retry_or_fail_job, queue, job, error})
  catch
    :exit, {:timeout, _} -> :timeout
  end

  def fail_job(pid, job, error) do
    GenServer.call(pid, {:fail_job, job, error})
  catch
    :exit, {:timeout, _} -> :timeout
  end

  def remove_job(pid, queue, job) do
    GenServer.call(pid, {:remove_job, queue, job})
  catch
    :exit, {:timeout, _} -> :timeout
  end

  def remove_retry(pid, job) do
    GenServer.call(pid, {:remove_retry, job})
  catch
    :exit, {:timeout, _} -> :timeout
  end

  def remove_backup(pid, queue, job) do
    GenServer.call(pid, {:remove_backup, queue, job})
  catch
    :exit, {:timeout, _} -> :timeout
  end

  def remove_backup_jobs(pid, jobs) do
    GenServer.call(pid, {:remove_backup_jobs, jobs})
  catch
    :exit, {:timeout, _} -> :timeout
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

  def handle_call({:fail_job, job, error}, _from, state) do
    response = Manager.fail_job(state.namespace, job, error)

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

  def handle_call({:remove_backup_jobs, jobs}, _from, state) do
    response = Manager.remove_backup_jobs(state.namespace, jobs)

    {:reply, response, state}
  end
end
