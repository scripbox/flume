defmodule Flume.Pipeline do
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
end
