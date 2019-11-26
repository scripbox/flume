defmodule Flume.Queue.API do
  @callback bulk_enqueue(String.t(), [any()], [any()]) :: {:ok, term} | {:error, String.t()}

  @callback enqueue(String.t(), Atom.t(), Atom.t(), [any()], [any()]) ::
              {:ok, term} | {:error, String.t()}

  @callback enqueue_in(String.t(), integer, Atom.t(), Atom.t(), [any()], [any()]) ::
              {:ok, term} | {:error, String.t()}

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
