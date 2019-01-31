defmodule Flume.Instrumentation.EventHandler do
  @moduledoc """
  This module acts as an interface for dispatching telemetry events.
  """
  @callback handle(atom(), integer(), any(), any()) :: any()
end
