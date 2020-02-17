defmodule Flume.Pipeline.DefaultAPI do
  @behaviour Flume.Pipeline.API

  alias Flume.Pipeline.Event

  @impl Flume.Pipeline.API
  def pause(pipeline_name, opts), do: Event.pause(pipeline_name, opts)

  @impl Flume.Pipeline.API
  def resume(pipeline_name, opts), do: Event.resume(pipeline_name, opts)
end
