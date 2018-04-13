defmodule Flume.Job do
  alias Flume.Event

  # Statuses
  @started :started
  @processed :processed
  @failed :failed
  @completed :completed

  def started, do: @started
  def processed, do: @processed
  def failed, do: @failed
  def completed, do: @completed

  defstruct status: nil, event: %Event{}, error_message: nil
end

