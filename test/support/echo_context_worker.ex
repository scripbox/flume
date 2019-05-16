defmodule EchoContextWorker do
  use Flume.API

  def perform([[caller]]), do: send_back_context(caller, :context)
  def perform(caller), do: String.to_atom(caller) |> send_back_context(:context)

  defp send_back_context(caller, scope), do: send(caller, {scope, worker_context()})
end
