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
    Logger.debug("#{state.name} [Consumer] received #{length(events)} events")

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
  defp notify(:completed, pipeline_name) do
    # decrements the :pending events count
    {:ok, _pending} = PipelineStats.decr(:pending, pipeline_name)
  end

  defp notify(:failed, pipeline_name) do
    # increments the :failed events count
    {:ok, _failed} = PipelineStats.incr(:failed, pipeline_name)
  end

  defp notify(:processed, pipeline_name) do
    # increments the :processed events count
    {:ok, _processed} = PipelineStats.incr(:processed, pipeline_name)
  end

  defp upstream_pipeline_name(pipeline_name) do
    Enum.join([pipeline_name, "producer_consumer"], "_")
    |> String.to_atom()
  end

  defp process_event(state, event) do
    [event.class] |> Module.safe_concat() |> apply(:perform, event.args)

    Logger.info("#{state.name} [Consumer] processed event #{event.jid}")
    notify(:processed, state.name)

    Flume.remove_backup(event.queue, event.original_json)
    {:ok, state}
  rescue
    e in _ ->
      Flume.retry_or_fail_job(event.queue, event.original_json, Kernel.inspect(e))
      Logger.error("#{state.name} [Consumer] failed with error: #{Kernel.inspect(e)}")
      notify(:failed, state.name)

      {:error, Kernel.inspect(e)}
  after
    notify(:completed, state.name)
  end
end
