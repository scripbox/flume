defmodule Flume.Pipeline.Event.Worker do
  @moduledoc """
  Processes each event dispatched from the previous pipeline stage.
  This stage acts as a Consumer in the GenStage pipeline.

  Producer <- ProducerConsumer <- ConsumerSupervisor <- [**Consumer**]
  """

  require Logger

  alias Flume.{Event, BulkEvent, Pipeline.SystemEvent}
  alias Flume.Pipeline.Event, as: EventPipeline

  @default_function_name "perform"

  # Client API
  def start_link(pipeline, event) do
    Task.start_link(__MODULE__, :process, [pipeline, event])
  end

  def process(pipeline, %BulkEvent{} = bulk_event) do
    Logger.debug("#{pipeline.name} [Consumer] received bulk event - #{inspect(bulk_event)}")

    do_process_event(pipeline, bulk_event)
  rescue
    e in [ArgumentError] ->
      notify(:completed, pipeline.name, length(bulk_event.events))

      Logger.error(
        "#{pipeline.name} [Consumer] error while processing event: #{Kernel.inspect(e)}"
      )
  end

  def process(pipeline, event) do
    Logger.debug("#{pipeline.name} [Consumer] received event - #{inspect(event)}")

    event = Event.decode!(event)

    do_process_event(pipeline, event)
  rescue
    e in [Jason.DecodeError, ArgumentError] ->
      notify(:completed, pipeline.name)
      Logger.error("#{pipeline.name} [Consumer] failed while parsing event: #{Kernel.inspect(e)}")
  end

  # Private API
  defp notify(_status, _pipeline_name, count \\ 1)

  defp notify(:completed, pipeline_name, count) do
    # decrements the :pending events count
    {:ok, _pending} = EventPipeline.Stats.decr(:pending, pipeline_name, count)
  end

  defp notify(:failed, pipeline_name, count) do
    # increments the :failed events count
    {:ok, _failed} = EventPipeline.Stats.incr(:failed, pipeline_name, count)
  end

  defp notify(:processed, pipeline_name, count) do
    # increments the :processed events count
    {:ok, _processed} = EventPipeline.Stats.incr(:processed, pipeline_name, count)
  end

  defp do_process_event(%{name: pipeline_name}, %Event{} = event) do
    function_name =
      Map.get(event, :function, @default_function_name)
      |> String.to_atom()

    [event.class]
    |> Module.safe_concat()
    |> apply(function_name, event.args)

    Logger.debug("#{pipeline_name} [Consumer] processed event: #{event.class} - #{event.jid}")
    notify(:processed, pipeline_name)

    SystemEvent.Producer.enqueue({:success, event})
  rescue
    e in _ ->
      error_message = Kernel.inspect(e)
      handle_failure(pipeline_name, event, error_message)
  catch
    :exit, {:timeout, message} ->
      handle_failure(pipeline_name, event, inspect(message))
  after
    notify(:completed, pipeline_name)
  end

  defp do_process_event(%{name: pipeline_name}, %BulkEvent{} = bulk_event) do
    function_name =
      Map.get(bulk_event, :function, @default_function_name)
      |> String.to_atom()

    [bulk_event.class]
    |> Module.safe_concat()
    |> apply(function_name, bulk_event.args)

    Logger.debug("#{pipeline_name} [Consumer] processed bulk event: #{bulk_event.class}")
    notify(:processed, pipeline_name, length(bulk_event.events))

    # Mark all events as success
    bulk_event.events
    |> Enum.map(fn event -> {:success, event} end)
    |> SystemEvent.Producer.enqueue()
  rescue
    e in _ ->
      error_message = Kernel.inspect(e)
      handle_failure(pipeline_name, bulk_event, error_message)
  catch
    :exit, {:timeout, message} ->
      handle_failure(pipeline_name, bulk_event, inspect(message))
  after
    notify(:completed, pipeline_name, length(bulk_event.events))
  end

  defp immediate_caller(current_process) do
    # collect stacktrace
    Process.info(current_process, :current_stacktrace)
    |> elem(1)
    |> Enum.map(&Exception.format_stacktrace_entry/1)
  end

  defp handle_failure(pipeline_name, %Event{} = event, error_message) do
    caller = immediate_caller(self())

    Logger.error(
      "#{pipeline_name} [Consumer] failed with error: #{error_message} - #{caller} - job - #{
        inspect(event.original_json)
      }"
    )

    notify(:failed, pipeline_name)

    SystemEvent.Producer.enqueue({:failed, event, error_message})
  end

  defp handle_failure(pipeline_name, %BulkEvent{} = bulk_event, error_message) do
    caller = immediate_caller(self())

    Logger.error(
      "#{pipeline_name} [Consumer] failed with error: #{error_message} - #{caller} - job - #{
        inspect(bulk_event.class)
      }"
    )

    notify(:failed, pipeline_name, length(bulk_event.events))

    bulk_event.events
    |> Enum.map(fn event -> {:failed, event, error_message} end)
    |> SystemEvent.Producer.enqueue()
  end
end
