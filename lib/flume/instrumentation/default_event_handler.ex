defmodule Flume.Instrumentation.DefaultEventHandler do
  @behaviour Flume.Instrumentation.EventHandler

  require Flume.Logger

  alias Flume.{Instrumentation, Logger}

  def handle(
        event_name,
        duration,
        %{pipeline_name: pipeline_name, module: module},
        nil
      ) do
    Logger.info("#{pipeline_name}/#{metric_path(event_name, module)} - #{duration}")
  end

  def handle(
        event_name,
        duration,
        %{pipeline_name: pipeline_name, module: module},
        app_name: app_name
      ) do
    Logger.info(
      "#{app_name}/#{pipeline_name}/#{metric_path(event_name, module)} - #{duration}"
    )
  end

  def handle(event_name, duration, %{pipeline_name: pipeline_name}, nil) do
    Logger.info("#{pipeline_name}/#{Instrumentation.format_event_name(event_name)} - #{duration}")
  end

  def handle(event_name, duration, %{pipeline_name: pipeline_name}, app_name: app_name) do
    Logger.info("#{app_name}/#{pipeline_name}/#{Instrumentation.format_event_name(event_name)} - #{duration}")
  end

  defp metric_path(event_name, nil), do: Instrumentation.format_event_name(event_name)

  defp metric_path(event_name, module) do
    formatted_event_name = Instrumentation.format_event_name(event_name)
    "#{formatted_event_name}/#{module}"
  end
end
