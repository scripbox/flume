defmodule Flume.Utils.IntegerExtension do
  def parse(value, default \\ :error)

  def parse(nil, nil), do: nil

  def parse(nil, default), do: default

  def parse(value, _default) when is_integer(value), do: value

  def parse(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {count, _} ->
        count

      :error ->
        default
    end
  end
end
