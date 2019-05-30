defmodule Flume.Utils do
  def payload_size(list) when is_list(list) do
    Enum.reduce(list, 0, fn item, acc -> acc + byte_size(item) end)
  end
end
