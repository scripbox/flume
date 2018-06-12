defmodule Flume.Event.Consumer do
  @moduledoc """
  A consumer will be consumer supervisor that will
  spawn Worker tasks for each event.
  """

  use ConsumerSupervisor

  alias Flume.Event.{Producer, Worker}

  def start_link do
    ConsumerSupervisor.start_link(__MODULE__, :ok)
  end

  # Callbacks

  def init(:ok) do
    children = [
      worker(Worker, [], restart: :temporary)
    ]

    {
      :ok,
      children,
      strategy: :one_for_one,
      subscribe_to: [{Producer, max_demand: 1000}]
    }
  end
end
