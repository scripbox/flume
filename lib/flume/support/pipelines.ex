defmodule Flume.Support.Pipelines do
  @moduledoc """
  This module returns the pipelines and its children
  based on the configuration
  """

  alias Flume.Pipeline.Event, as: EventPipeline

  @doc false
  def list do
    import Supervisor.Spec

    # initialize the :ets table to store pipeline stats
    EventPipeline.Stats.init()

    Flume.Config.pipelines()
    |> Enum.flat_map(fn pipeline ->
      pipeline_struct = Flume.Pipeline.new(pipeline)

      [
        worker(EventPipeline.Producer, [pipeline_struct], id: generate_id()),
        worker(EventPipeline.ProducerConsumer, [pipeline_struct], id: generate_id()),
        worker(EventPipeline.Consumer, [pipeline_struct], id: generate_id())
      ]
    end)
  end

  defp generate_id do
    <<part1::32, part2::32>> = :crypto.strong_rand_bytes(8)
    "#{part1}#{part2}"
  end
end
