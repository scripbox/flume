defmodule Flume.Job do
  # Statuses
  @started :started
  @processed :processed

  def started, do: @started
  def processed, do: @processed

  defstruct status: nil, event: nil, error_message: nil
end
