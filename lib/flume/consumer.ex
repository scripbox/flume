defmodule Flume.Consumer do
  @moduledoc """
  Processes each event dispatched from the previous pipeline stage.
  This stage acts as a Consumer in the GenStage pipeline.

  Producer <- ProducerConsumer <- ConsumerSupervisor <- [**Consumer**]
  """

  require Logger

  alias Flume.{Event, PipelineStats}

  @default_function_name "perform"

  # Client API
  def start_link(pipeline, event) when is_binary(event) do
    Task.start_link(__MODULE__, :process, [pipeline, event])
  end

  def start_link(_pipeline, {:success, %Event{} = event}) do
    Task.start_link(__MODULE__, :success, [event])
  end

  def start_link(_pipeline, {:failed, %Event{} = event, exception_message}) do
    Task.start_link(__MODULE__, :fail, [event, exception_message])
  end

  def process(pipeline, event) do
    Logger.debug("#{pipeline.name} [Consumer] received 1 event")

    event = Event.decode!(event)

    do_process_event(pipeline, event)
  rescue
    e in Poison.SyntaxError ->
      Logger.error("#{pipeline.name} [Consumer] failed while parsing event: #{Kernel.inspect(e)}")
  end

  def success(event) do
    Flume.remove_backup(event.queue, event.original_json)
  end

  def fail(event, exception_message) do
    Flume.retry_or_fail_job(event.queue, event.original_json, exception_message)
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

  defp producer(pipeline_name) do
    :"#{pipeline_name}_producer"
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

    GenServer.cast(producer(pipeline_name), {:events, [{:success, event}]})
  rescue
    e in _ ->
      caller = immediate_caller(self())
      exception_message = Kernel.inspect(e)

      Logger.error(
        "#{pipeline_name} [Consumer] failed with error: #{exception_message} - #{caller} - job - #{
          inspect(event.original_json)
        }"
      )

      notify(:failed, pipeline_name)

      GenServer.cast(producer(pipeline_name), {:events, [{:failed, event, exception_message}]})
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
end
