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

  def perform(args) when is_list(args) do
    args
    |> Enum.each(fn [owner, message] ->
      send(owner, {:received, message})
    end)
  end
end
