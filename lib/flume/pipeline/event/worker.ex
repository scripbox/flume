defmodule Flume.Pipeline.Event.Worker do
  @moduledoc """
  Processes each event dispatched from the previous pipeline stage.
  This stage acts as a Consumer in the GenStage pipeline.

  Producer <- ProducerConsumer <- ConsumerSupervisor <- [**Consumer**]
  """

  require Logger

  alias Flume.{Event, Pipeline.SystemEvent}

  @default_function_name "perform"

  # Client API
  def start_link(pipeline, event) do
    Task.start_link(__MODULE__, :process, [pipeline, event])
  end

  def process(%{name: pipeline_name, timeout: timeout} = pipeline, event) do
    event = Event.decode!(event)
    :timer.apply_after(timeout, __MODULE__, :timeout, [self(), pipeline_name, event])
    Logger.debug("#{pipeline_name} [Event.Worker] received 1 event")
    do_process_event(pipeline, event)
  rescue
    e in Poison.SyntaxError ->
      Logger.error("#{pipeline_name} [Event.Worker] failed while parsing event: #{Kernel.inspect(e)}")
  end

  def timeout(pid, pipeline_name, event) do
    if Process.alive?(pid) && Process.exit(pid, :kill) do
      handle_failure(pipeline_name, event, :timeout)
    end
  end

  defp do_process_event(%{name: pipeline_name}, event) do
    function_name =
      Map.get(event, :function, @default_function_name)
      |> String.to_atom()

    [event.class]
    |> Module.safe_concat()
    |> apply(function_name, event.args)

    Logger.debug("#{pipeline_name} [Event.Worker] processed event: #{event.class} - #{event.jid}")

    SystemEvent.Producer.enqueue({:success, event})
  rescue
    e in _ ->
      error_message = Kernel.inspect(e)
      handle_failure(pipeline_name, event, error_message)
  catch
    :exit, {:timeout, message} ->
      handle_failure(pipeline_name, event, inspect(message))
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
      "#{pipeline_name} [Event.Worker] failed with error: #{error_message} - #{caller} - job - #{
        inspect(event.original_json)
      }"
    )

    SystemEvent.Producer.enqueue({:failed, event, error_message})
  end
end
