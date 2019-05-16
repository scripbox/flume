defmodule Flume.Pipeline.Context do
  @namespace "flume_worker_context"

  def put(nil), do: nil

  def put(context) do
    Process.put(@namespace, context)
  end

  def get do
    Process.get(@namespace)
  end
end
