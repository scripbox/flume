defmodule Flume.Pipeline.Event.Processor do
  @moduledoc """
  Processes each event dispatched from the previous pipeline stage.
  This stage acts as a Consumer in the GenStage pipeline.

  Producer <- ProducerConsumer <- ConsumerSupervisor <- [**Consumer**]
  """

  require Logger

  alias Flume.{Event, Pipeline.SystemEvent}
  alias Flume.Pipeline.Event, as: EventPipeline

  @default_function_name "perform"

  # Client API
  def start_link(pipeline, event) do
    Task.start_link(__MODULE__, :process, [pipeline, event])
  end

  def process(pipeline, event) do
    Logger.debug("#{pipeline.name} [Consumer] received 1 event")

    event = Event.decode!(event)

    do_process_event(pipeline, event)
  rescue
    e in Poison.SyntaxError ->
      Logger.error("#{pipeline.name} [Consumer] failed while parsing event: #{Kernel.inspect(e)}")
  end

  # Private API
  defp notify(:completed, pipeline_name) do
    # decrements the :pending events count
    {:ok, _pending} = EventPipeline.Stats.decr(:pending, pipeline_name)
  end

  defp notify(:failed, pipeline_name) do
    # increments the :failed events count
    {:ok, _failed} = EventPipeline.Stats.incr(:failed, pipeline_name)
  end

  defp notify(:processed, pipeline_name) do
    # increments the :processed events count
    {:ok, _processed} = EventPipeline.Stats.incr(:processed, pipeline_name)
  end

  defp do_process_event(%{name: pipeline_name}, event) do
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

  defp immediate_caller(current_process) do
    # collect stacktrace
    Process.info(current_process, :current_stacktrace)
    |> elem(1)
    |> Enum.at(2)
    |> Exception.format_stacktrace_entry()
  end

  defp handle_failure(pipeline_name, event, error_message) do
    caller = immediate_caller(self())

    Logger.error(
      "#{pipeline_name} [Consumer] failed with error: #{error_message} - #{caller} - job - #{
        inspect(event.original_json)
      }"
    )

    notify(:failed, pipeline_name)

    SystemEvent.Producer.enqueue({:failed, event, error_message})
  end
end
