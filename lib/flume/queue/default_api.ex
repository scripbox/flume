defmodule Flume.Queue.DefaultAPI do
  @behaviour Flume.Queue.API

  alias Flume.Config
  alias Flume.Queue.Manager

  def bulk_enqueue(queue, jobs, opts \\ []) do
    Manager.bulk_enqueue(namespace(), queue, jobs, opts)
  end

  def enqueue(
        queue,
        worker,
        function_name \\ :perform,
        args,
        opts \\ []
      ) do
    Manager.enqueue(
      namespace(),
      queue,
      worker,
      function_name,
      args,
      opts
    )
  end

  def enqueue_in(
        queue,
        time_in_seconds,
        worker,
        function_name \\ :perform,
        args,
        opts \\ []
      ) do
    Manager.enqueue_in(
      namespace(),
      queue,
      time_in_seconds,
      worker,
      function_name,
      args,
      opts
    )
  end

  defp namespace, do: Config.namespace()
end
