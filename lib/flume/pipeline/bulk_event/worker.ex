defmodule Flume.Pipeline.BulkEvent.Worker do
  require Flume.{Instrumentation, Logger}

  alias Flume.{BulkEvent, Instrumentation, Logger}
  alias Flume.Pipeline.Event, as: EventPipeline
  alias Flume.Pipeline.SystemEvent, as: SystemEventPipeline

  def process(%{name: pipeline_name} = pipeline, %BulkEvent{class: class} = bulk_event) do
    {duration, _} =
      Instrumentation.measure do
        Logger.debug("#{pipeline_name} [Consumer] received bulk event - #{inspect(bulk_event)}")

        do_process_event(pipeline, bulk_event)
      end

    Instrumentation.execute(
      [:worker, :duration],
      duration,
      %{module: Instrumentation.format_module(class), pipeline_name: pipeline_name},
      pipeline[:instrument]
    )
  rescue
    e in [ArgumentError] ->
      EventPipeline.update_completed(pipeline_name, length(bulk_event.events))

      Logger.error(
        "#{pipeline_name} [Consumer] error while processing event: #{Kernel.inspect(e)}"
      )
  end

  defp do_process_event(%{name: pipeline_name} = pipeline, %BulkEvent{
         class: class,
         function: function,
         args: args,
         events: events
       }) do
    function_name = String.to_atom(function)

    {duration, _} =
      Instrumentation.measure do
        [class]
        |> Module.safe_concat()
        |> apply(function_name, args)
      end

    Instrumentation.execute(
      [:worker, :job, :duration],
      duration,
      %{module: Instrumentation.format_module(class), pipeline_name: pipeline_name},
      pipeline[:instrument]
    )

    Logger.debug("#{pipeline_name} [Consumer] processed bulk event: #{class}")

    EventPipeline.update_processed(pipeline_name, length(events))
    # Mark all events as success
    events
    |> Enum.map(fn event -> {:success, event} end)
    |> SystemEventPipeline.enqueue()
  rescue
    e in _ ->
      error_message = Kernel.inspect(e)
      handle_failure(pipeline_name, class, events, error_message)
  catch
    :exit, {:timeout, message} ->
      handle_failure(pipeline_name, class, events, inspect(message))
  after
    EventPipeline.update_completed(pipeline_name, length(events))
  end

  defp handle_failure(pipeline_name, class, events, error_message) do
    Logger.error("#{pipeline_name} [Consumer] failed with error: #{error_message}", %{
      worker_name: class
    })

    EventPipeline.update_failed(pipeline_name, length(events))

    events
    |> Enum.map(fn event -> {:failed, event, error_message} end)
    |> SystemEventPipeline.enqueue()
  end
end
