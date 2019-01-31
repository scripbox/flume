defmodule Flume.Instrumentation do
  defdelegate attach(handler_id, event_name, function, config), to: :telemetry

  defdelegate attach_many(handler_id, event_names, function, config), to: :telemetry

  def execute(event_name, event_value, true = _instrument) do
    :telemetry.execute(event_name, event_value)
  end

  # Skip invoking telemetry when instrument is not true
  def execute(_event_name, _event_value, _instrument), do: nil

  def execute(event_name, event_value, metadata, true = _instrument) do
    :telemetry.execute(event_name, event_value, metadata)
  end

  # Skip invoking telemetry when instrument is not true
  def execute(_event_name, _event_value, _metadata, _instrument), do: nil

  defmacro measure(do: yield) do
    quote do
      start_time = Flume.Instrumentation.unix_seconds()
      result = unquote(yield)
      duration = Flume.Instrumentation.unix_seconds() - start_time
      {duration, result}
    end
  end

  def unix_seconds(unit \\ :milliseconds), do: DateTime.to_unix(DateTime.utc_now(), unit)

  def format_module(module_name) do
    module_name |> to_string() |> String.replace(".", "_") |> String.downcase()
  end

  def format_event_name([]), do: ""

  def format_event_name([name | other_names]) do
    Path.join("#{name}", format_event_name(other_names))
  end
end
