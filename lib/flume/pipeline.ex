defmodule Flume.Pipeline do
  alias Flume.Pipeline.Control
  alias Flume.Utils.IntegerExtension

  @default_max_demand 500

  defstruct [
    :name,
    :queue,
    :rate_limit_count,
    :rate_limit_scale,
    :rate_limit_key,
    :max_demand,
    :batch_size,
    :paused,
    :producer,
    :instrument
  ]

  def new(%{name: name, queue: queue} = opts) do
    batch_size = IntegerExtension.parse(opts[:batch_size], nil)
    rate_limit_count = IntegerExtension.parse(opts[:rate_limit_count], nil)
    rate_limit_scale = IntegerExtension.parse(opts[:rate_limit_scale], nil)
    max_demand = IntegerExtension.parse(opts[:max_demand], @default_max_demand)

    %__MODULE__{
      name: name,
      queue: queue,
      rate_limit_count: rate_limit_count,
      rate_limit_scale: rate_limit_scale,
      rate_limit_key: opts[:rate_limit_key],
      max_demand: max_demand,
      batch_size: batch_size,
      instrument: opts[:instrument]
    }
  end

  def pause(pipeline_name, options \\ []) do
    with {:ok, pipeline_name} <- validate_pipeline_name(pipeline_name),
         {:ok, options} <- Control.Options.sanitized_options(options) do
      apply(Flume.Config.pipeline_api_module(), :pause, [pipeline_name, options])
    end
  end

  def resume(pipeline_name, options \\ []) do
    with {:ok, pipeline_name} <- validate_pipeline_name(pipeline_name),
         {:ok, options} <- Control.Options.sanitized_options(options) do
      apply(Flume.Config.pipeline_api_module(), :resume, [pipeline_name, options])
    end
  end

  defp validate_pipeline_name(pipeline_name) when is_atom(pipeline_name),
    do: validate_pipeline_name(to_string(pipeline_name))

  defp validate_pipeline_name(pipeline_name) when is_binary(pipeline_name) do
    if Enum.any?(Flume.Config.pipeline_names(), &(&1 == pipeline_name)) do
      {:ok, pipeline_name}
    else
      {:error, "pipeline #{pipeline_name} has not been configured"}
    end
  end

  defp validate_pipeline_name(_) do
    {:error, "invalid value for a pipeline name"}
  end
end
