defmodule Flume.Pipeline do
  alias Flume.Utils.IntegerExtension

  @default_rate_limit_count 1000
  @default_rate_limit_scale 5000

  defstruct [
    :name,
    :queue,
    :interval,
    :max_demand,
    :batch_size,
    :paused,
    :producer,
    :instrument
  ]

  def new(%{name: name, queue: queue} = pipeline) do
    batch_size = IntegerExtension.parse(pipeline[:batch_size], nil)
    max_demand = IntegerExtension.parse(pipeline[:rate_limit_count], @default_rate_limit_count)
    interval = IntegerExtension.parse(pipeline[:rate_limit_scale], @default_rate_limit_scale)

    %__MODULE__{
      name: name,
      queue: queue,
      interval: interval,
      max_demand: max_demand,
      batch_size: batch_size,
      instrument: pipeline[:instrument]
    }
  end
end
