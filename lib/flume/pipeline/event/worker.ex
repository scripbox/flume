defmodule Flume.Pipeline.Event.Worker do
  @moduledoc """
  Processes each event dispatched from the previous pipeline stage.
  This stage acts as a Consumer in the GenStage pipeline.

  Producer <- ProducerConsumer <- ConsumerSupervisor <- [**Consumer**]
  """

  require Flume.{Instrumentation, Logger}

  alias Flume.{BulkEvent, Event, Instrumentation, Logger}
  alias Flume.Pipeline.BulkEvent, as: BulkEventPipeline
  alias Flume.Pipeline.SystemEvent, as: SystemEventPipeline

  # Client API
  def start_link(pipeline, %BulkEvent{} = bulk_event) do
    Task.start_link(BulkEventPipeline.Worker, :process, [pipeline, bulk_event])
  end

  def start_link(pipeline, event) do
    Task.start_link(__MODULE__, :process, [pipeline, event])
  end

  def process(%{name: pipeline_name} = pipeline, event) do
    {duration, %Event{class: class}} =
      Instrumentation.measure do
        Logger.debug("#{pipeline_name} [Consumer] received event - #{inspect(event)}")

        event = Event.decode!(event)

        do_process_event(pipeline, event)
        event
      end

    Instrumentation.execute(
      [String.to_atom(pipeline_name), :worker, :duration],
      duration,
      %{module: Instrumentation.format_module(class)},
      pipeline[:instrument]
    )
  rescue
    e in [Jason.DecodeError, ArgumentError] ->
      Logger.error("#{pipeline.name} [Consumer] failed while parsing event: #{Kernel.inspect(e)}")
  end

  defp do_process_event(
         %{name: pipeline_name} = pipeline,
         %Event{function: function, class: class, args: args, jid: jid} = event
       ) do
    function_name = String.to_atom(function)

    {duration, _} =
      Instrumentation.measure do
        [class]
        |> Module.safe_concat()
        |> apply(function_name, args)
      end

    Instrumentation.execute(
      [String.to_atom(pipeline_name), :worker, :job, :duration],
      duration,
      %{module: Instrumentation.format_module(class)},
      pipeline[:instrument]
    )

    Logger.debug("#{pipeline_name} [Consumer] processed event: #{class} - #{jid}")

    SystemEventPipeline.enqueue({:success, event})
  rescue
    e in _ ->
      error_message = Kernel.inspect(e)
      handle_failure(pipeline_name, event, error_message)
  catch
    :exit, {:timeout, message} ->
      handle_failure(pipeline_name, event, inspect(message))
  end

  defp handle_failure(
         pipeline_name,
         %Event{class: class, function: function, args: args, retry_count: retry_count} = event,
         error_message
       ) do
    Logger.error("#{pipeline_name} [Consumer] failed with error: #{error_message}", %{
      worker_name: class,
      function: function,
      args: args,
      retry_count: retry_count
    })

    SystemEventPipeline.enqueue({:failed, event, error_message})
  end
end
