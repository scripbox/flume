defmodule Flume.Support.JobParser do
  def encode!(job), do: Poison.encode!(job)

  def decode!(job), do: Poison.decode!(job)
end
