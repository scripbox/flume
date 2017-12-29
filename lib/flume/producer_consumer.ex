defmodule Flume.ProducerConsumer do
  @moduledoc """
  Takes a batch of events periodically to be sent to the consumers.
  This stage acts as a Producer-Consumer in the GenStage pipeline.

  Producer <- [**ProducerConsumer**] <- Consumer
  """
  use GenStage

  require Logger

  # Client API
  def start_link(state \\ %{}) do
    GenStage.start_link(__MODULE__, state, name: process_name(state.name))
  end

  # Server callbacks
  def init(state) do
    upstream = upstream_process_name(state.name)

    {:producer_consumer, state, subscribe_to: [{upstream, min_demand: 0, max_demand: state.max_demand}]}
  end

  def handle_subscribe(:producer, _opts, from, state) do
    # We will only allow :max_demand events every :interval milliseconds
    pending = state.max_demand
    interval = state.interval

    # Register the producer in the state
    state = Map.put(state, from, {pending, interval})
    # Ask for the pending events and schedule the next time around
    state = ask_and_schedule(state, from)

    # Returns manual as we want control over the demand
    {:manual, state}
  end

  def handle_subscribe(:consumer, _opts, _from, state) do
    {:automatic, state}
  end

  def handle_events(events, from, state) do
    Logger.info("#{state.name} [ProducerConsumer] received #{length events} events")

    {:noreply, events, state}
  end

  def handle_info({:ask, from}, state) do
    # This callback is invoked by the Process.send_after/3 message below.
    {:noreply, [], ask_and_schedule(state, from)}
  end

  # Private API
  defp ask_and_schedule(state, from) do
    case state do
      %{^from => {pending, interval}} ->
        Logger.info("#{state.name} [ProducerConsumer] asking #{pending} events")
        # Request a batch of events with a max batch size
        GenStage.ask(from, pending)
        # Schedule the next request
        Process.send_after(self(), {:ask, from}, interval)
        state
      %{} ->
        state
    end
  end

  defp process_name(pipeline_name) do
    Enum.join([pipeline_name, "producer_consumer"], "_")
    |> String.to_atom
  end

  defp upstream_process_name(pipeline_name) do
    Enum.join([pipeline_name, "producer"], "_")
    |> String.to_atom
  end
end
