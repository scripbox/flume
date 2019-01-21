defmodule Flume.Pipeline.BulkEvent.Worker do
  require Flume.Logger

  alias Flume.{BulkEvent, Logger}
  alias Flume.Pipeline.Event, as: EventPipeline
  alias Flume.Pipeline.SystemEvent, as: SystemEventPipeline

  def process(%{name: pipeline_name} = pipeline, %BulkEvent{} = bulk_event) do
    Logger.debug("#{pipeline_name} [Consumer] received bulk event - #{inspect(bulk_event)}")

    do_process_event(pipeline, bulk_event)
  rescue
    e in [ArgumentError] ->
      EventPipeline.update_completed(pipeline_name, length(bulk_event.events))

      Logger.error(
        "#{pipeline_name} [Consumer] error while processing event: #{Kernel.inspect(e)}"
      )
  end

  defp do_process_event(%{name: pipeline_name}, %BulkEvent{
         class: class,
         function: function,
         args: args,
         events: events
       }) do
    function_name = String.to_atom(function)

    [class]
    |> Module.safe_concat()
    |> apply(function_name, args)

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
