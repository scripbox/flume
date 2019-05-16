defmodule Flume.Event do
  @moduledoc """
  This module is responsible for decoding the
  serialized event received by the consumer stages.
  """

  @default_function_name "perform"

  # Sample Event Schema
  # {
  #   "class": "Elixir.Worker",
  #   "function": "perform",
  #   "queue": "test",
  #   "jid": "1082fd87-2508-4eb4-8fba-2958584a60e3",
  #   "args": [1],
  #   "retry_count": 1,
  #   "enqueued_at": 1514367662,
  #   "finished_at": 1514367664,
  #   "failed_at": null,
  #   "retried_at": null,
  #   "error_message": "<Error Message>",
  #   "error_backtrace": "error backtrace"
  # }
  @keys [
    class: nil,
    function: @default_function_name,
    queue: nil,
    jid: nil,
    args: [],
    retry_count: 0,
    enqueued_at: nil,
    finished_at: nil,
    failed_at: nil,
    retried_at: nil,
    error_message: nil,
    error_backtrace: nil,
    context: nil
  ]

  @type t :: %__MODULE__{
          class: String.t() | atom,
          function: String.t(),
          queue: String.t(),
          jid: String.t(),
          args: List.t(),
          retry_count: non_neg_integer,
          enqueued_at: DateTime.t(),
          finished_at: DateTime.t(),
          failed_at: DateTime.t(),
          retried_at: DateTime.t(),
          error_message: String.t(),
          error_backtrace: String.t(),
          context: map()
        }

  @derive {Jason.Encoder, only: Keyword.keys(@keys)}
  defstruct [:original_json | @keys]

  @doc false
  def new(attributes) when is_map(attributes) do
    struct(__MODULE__, %{
      class: attributes["class"],
      function: attributes["function"],
      queue: attributes["queue"],
      jid: attributes["jid"],
      args: attributes["args"],
      retry_count: attributes["retry_count"],
      enqueued_at: attributes["enqueued_at"],
      finished_at: attributes["finished_at"],
      failed_at: attributes["failed_at"],
      retried_at: attributes["retried_at"],
      error_message: attributes["error_message"],
      error_backtrace: attributes["error_backtrace"],
      context: attributes["context"]
    })
  end

  def new(_) do
    struct(__MODULE__, %{})
  end

  @doc """
  Decode the JSON payload storing the original json as part of the struct.
  """
  @spec decode(binary) :: {:ok, %__MODULE__{}} | {:error, Jason.DecodeError.t()}
  def decode(payload) do
    case Jason.decode(payload) do
      {:ok, %{"args" => %{}} = response} ->
        {:ok, %__MODULE__{__MODULE__.new(response) | original_json: payload, args: []}}

      {:ok, response} ->
        %__MODULE__{__MODULE__.new(response) | original_json: payload}

      {:error, error} ->
        {:error, error}

      {:error, :invalid, pos} ->
        {:error, "Invalid json at position: #{pos}"}
    end
  end

  @doc """
  Decode the JSON payload storing the original json as part of the struct, raising if there is an error
  """
  @spec decode!(binary) :: %__MODULE__{}
  def decode!(payload) do
    case Jason.decode!(payload) do
      %{"args" => %{}} = response ->
        %__MODULE__{__MODULE__.new(response) | original_json: payload, args: []}

      response ->
        %__MODULE__{__MODULE__.new(response) | original_json: payload}
    end
  end
end
