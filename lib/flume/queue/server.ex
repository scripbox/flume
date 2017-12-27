defmodule Flume.Queue.Server do

  alias Flume.Queue.Manager

  defmodule State do
    defstruct namespace: nil, poll_timeout: nil
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    {
      :ok,
      %State{
        namespace: opts[:namespace],
        poll_timeout: opts[:poll_timeout]
      }
    }
  end

  def handle_call({:enqueue, queue, worker, args}, _from, state) do
    response = Manager.enqueue(state.namespace, queue, worker, args)
    {:reply, response, state}
  end

  def handle_call({:dequeue, queue, job}, _from, state) do
    response = Manager.dequeue(state.namespace, queue, job)
    {:reply, response, state}
  end
end
