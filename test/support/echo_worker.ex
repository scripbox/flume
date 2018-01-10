defmodule EchoWorker do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts \\ []) do
    {:ok, opts}
  end

  def perform(owner, message) do
    send(String.to_atom(owner), {:received, message})
  end
end
