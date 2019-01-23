defmodule Flume.Pipeline.SystemEvent do
  alias Flume.Pipeline.SystemEvent

  defdelegate enqueue(event_or_events), to: SystemEvent.Producer

  def pending_workers_count do
    Supervisor.which_children(SystemEvent.Supervisor)
    |> Enum.filter(fn {module, _, _, _} -> module == SystemEvent.Consumer end)
    |> Enum.reduce(0, fn {_, pid, _, _}, acc ->
      acc + SystemEvent.Consumer.workers_count(pid)
    end)
  end
end
