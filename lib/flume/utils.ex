defmodule Flume.Utils do
  def payload_size(list) when is_list(list) do
    Enum.reduce(list, 0, fn item, acc -> acc + byte_size(item) end)
  end

  def safe_apply(function, timeout) do
    task = Task.Supervisor.async_nolink(Flume.SafeApplySupervisor, function)

    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, result} -> {:ok, result}
      {:exit, reason} -> {:exit, reason}
      nil -> {:timeout, "Timed out after #{timeout} ms"}
    end
  end
end
