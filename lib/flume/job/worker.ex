defmodule Flume.Job.Worker do
  use GenServer

  require Logger

  alias Flume.{Job, Event, PipelineStats}
  alias Flume.Job.Manager

  @timeout "Timeout"

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: :"worker_#{random_id()}")
  end

  def execute(pid, event, pipeline_name) do
    GenServer.cast(pid, {:execute, event, pipeline_name})
  end

  def stop(pid) do
    GenServer.cast(pid, :stop)
  end

  def init(opts) do
    {:ok, opts}
  end

  # Client API
  def handle_cast({:execute, event, pipeline_name}, state) do
    event
    |> Event.decode!()
    |> do_execute(pipeline_name)

    {:noreply, state}
  rescue
    e in Poison.SyntaxError ->
      Logger.error("#{pipeline_name} [Consumer] failed while parsing event: #{Kernel.inspect(e)}")

      {:noreply, state}
  after
    stop(self())
  end

  def handle_cast(:stop, state) do
    {:stop, :normal, state}
  end

  # Helpers
  defp do_execute(event, pipeline_name) do
    function_name = event.function |> String.to_atom()

    Manager.monitor(self(), %Job{status: Job.started()})

    [event.class]
    |> Module.safe_concat()
    |> apply(function_name, event.args)

    Manager.monitor(self(), %Job{status: Job.processed()})

    Logger.debug("#{pipeline_name} [Consumer] processed event: #{event.class} - #{event.jid}")
    notify(:processed, pipeline_name)
  rescue
    e in _ ->
      failed(pipeline_name, event, Kernel.inspect(e))
  catch
    :exit, {:timeout, _} -> failed(pipeline_name, event, @timeout)
  after
    notify(:completed, pipeline_name)
  end

  defp stacktrace do
    Process.info(self(), :current_stacktrace)
    |> elem(1)
    |> Enum.at(2)
    |> Exception.format_stacktrace_entry()
  end

  defp failed(pipeline_name, event, error_message) do
    Manager.failed(self(), error_message)

    Logger.error(
      "#{pipeline_name} [Consumer] failed with error: #{error_message} - #{stacktrace()} - job - #{
        inspect(event.original_json)
      }"
    )

    notify(:failed, pipeline_name)

    {:error, error_message}
  end

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

  defp random_id do
    <<part1::32, part2::32>> = :crypto.strong_rand_bytes(8)
    "#{part1}#{part2}"
  end
end
