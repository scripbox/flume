defmodule Flume.Instrumentation.DefaultEventHandler do
  @behaviour Flume.Instrumentation.EventHandler

  require Flume.Logger

  alias Flume.{Instrumentation, Logger}

  def handle(
        event_name,
        %{value: value},
        %{module: module},
        nil
      ) do
    Logger.info("#{metric_path(event_name, module)} - #{value}")
  end

  def handle(
        event_name,
        %{value: value},
        %{module: module},
        app_name: app_name
      ) do
    Logger.info("#{app_name}/#{metric_path(event_name, module)} - #{value}")
  end

  def handle(event_name, %{value: value}, _metadata, nil) do
    Logger.info("#{Instrumentation.format_event_name(event_name)} - #{value}")
  end

  def handle(event_name, %{value: value}, _metadata, app_name: app_name) do
    Logger.info("#{app_name}/#{Instrumentation.format_event_name(event_name)} - #{value}")
  end

  def handle(_, _, _, _) do
    :ok
  end

  defp metric_path(event_name, nil), do: Instrumentation.format_event_name(event_name)

  defp metric_path(event_name, module) do
    formatted_event_name = Instrumentation.format_event_name(event_name)
    "#{formatted_event_name}/#{module}"
  end
end
