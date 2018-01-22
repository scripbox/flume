defmodule Flume.PipelineStatsSync do
  @moduledoc """
  This process will persist to Redis the following information per pipeline:
    * Number of pending events
    * Number of processed events
    * Number of failed events
  """
  use GenServer
  require Logger
  alias Flume.PipelineStats

  # milliseconds
  @persist_interval 10_000

  # Public API
  def start_link() do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def init(state) do
    Process.send_after(self(), {:"$gen_cast", :persist_stats}, @persist_interval)
    {:ok, state}
  end

  def handle_cast(:persist_stats, state) do
    case PipelineStats.persist() do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "[PipelineStats] failed to persist stats to Redis. Reason: #{inspect(reason)}"
        )
    end

    Process.send_after(self(), {:"$gen_cast", :persist_stats}, @persist_interval)
    {:noreply, state}
  end
end
