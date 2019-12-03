defmodule Flume.Queue.API do
  @callback bulk_enqueue(String.t(), [any()], [any()]) :: {:ok, term} | {:error, String.t()}

  @callback enqueue(String.t(), Atom.t(), Atom.t(), [any()], [any()]) ::
              {:ok, term} | {:error, String.t()}

  @callback enqueue_in(String.t(), integer, Atom.t(), Atom.t(), [any()], [any()]) ::
              {:ok, term} | {:error, String.t()}
end
