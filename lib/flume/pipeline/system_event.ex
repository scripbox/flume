defmodule Flume.Pipeline.SystemEvent do
  alias Flume.Pipeline.SystemEvent

  defdelegate enqueue(event_or_events), to: SystemEvent.Producer
end
