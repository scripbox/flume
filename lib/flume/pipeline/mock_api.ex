defmodule Flume.Pipeline.MockAPI do
  @behaviour Flume.Pipeline.API

  @impl Flume.Pipeline.API
  def pause(pipeline_name, options) do
    send(self(), %{pipeline_name: pipeline_name, action: :pause, options: options})
  end

  @impl Flume.Pipeline.API
  def resume(pipeline_name, options) do
    send(self(), %{pipeline_name: pipeline_name, action: :resume, options: options})
  end
end
