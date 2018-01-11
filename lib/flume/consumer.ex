defmodule Flume.Consumer do
  @moduledoc """
  Processes each event dispatched from the previous pipeline stage.
  This stage acts as a Consumer in the GenStage pipeline.

  Producer <- ProducerConsumer <- [**Consumer**]
  """
  use GenStage

  require Logger
  alias Flume.PipelineStats

  # Client API
  def start_link(state \\ %{}) do
    GenStage.start_link(__MODULE__, state)
  end

  # Server Callbacks
  def init(state) do
    upstream = upstream_pipeline_name(state.name)
    {:consumer, state, subscribe_to: [{upstream, min_demand: 0, max_demand: 1}]}
  end

  def handle_events(events, _from, state) do
    Logger.info("#{state.name} [Consumer] received #{length events} events")

    # events will always be of size 1 as consumer has max_demand of 1
    [event | _] = events
    event = Flume.Event.decode!(event)

    process_event(state, event)

    {:noreply, [], state}
  rescue
    e in Poison.SyntaxError ->
      Logger.error("#{state.name} [Consumer] failed while parsing event: #{e.reason}")
      {:noreply, [], state}
  end

  # Private API
  defp notify_done(pipeline_name) do
    # The consumer decrements the pending events in :ets by 1
    {:ok, _pending} = PipelineStats.decr(:pending, pipeline_name)
  end

  defp upstream_pipeline_name(pipeline_name) do
    Enum.join([pipeline_name, "producer_consumer"], "_")
    |> String.to_atom
  end

  defp process_event(state, event) do
    [event.class] |> Module.safe_concat |> apply(:perform, event.args)

    Logger.info("#{state.name} [Consumer] finished processing event #{event.jid}")

    {:ok, state}
  rescue
    e in _ ->
      Flume.retry_or_fail_job(event.queue, event.original_json, e.reason)
      Logger.error("#{state.name} [Consumer] failed with error: #{e.reason}")
      {:error, e.reason}
  after
    notify_done(state.name)
  end
end
