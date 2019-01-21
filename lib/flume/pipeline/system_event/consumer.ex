defmodule Flume.Pipeline.SystemEvent.Consumer do
  @moduledoc """
  A consumer will be consumer supervisor that will
  spawn Worker tasks for each event.
  """

  use ConsumerSupervisor

  alias Flume.Pipeline.SystemEvent.{Producer, Worker}

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
      strategy: :one_for_one, subscribe_to: [{Producer, max_demand: 1000}]
    }
  end

  def workers_count(process_name) do
    %{workers: workers_count} = ConsumerSupervisor.count_children(process_name)
    workers_count
  end
end
