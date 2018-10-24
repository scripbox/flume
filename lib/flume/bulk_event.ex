defmodule Flume.BulkEvent do
  @moduledoc """
  This module is responsible for creating the bulk event
  received by the consumer stages.
  """
  # Sample Event Schema
  # {
  #   "class": "Elixir.Worker",
  #   "function": "perform",
  #   "queue": "test",
  #   "args": [[["arg_1"], ["arg_2"]],
  #   "events": []
  # }

  alias Flume.Event

  @keys [
    class: nil,
    function: nil,
    queue: nil,
    args: [],
    events: []
  ]

  @type t :: %__MODULE__{
          class: String.t() | atom,
          function: String.t(),
          queue: String.t(),
          args: List.t(),
          events: List.t()
        }

  defstruct @keys

  # @doc false
  def new(%Event{} = event) do
    struct(__MODULE__, %{
      class: event.class,
      function: event.function,
      queue: event.queue,
      args: [[event.args]],
      events: [event]
    })
  end

  def new(_) do
    struct(__MODULE__, %{})
  end

  def append(%__MODULE__{args: [bulk_args]} = bulk_event, %Event{} = event) do
    bulk_event
    |> Map.merge(%{
      args: [bulk_args ++ [event.args]],
      events: bulk_event.events ++ [event]
    })
  end
end
