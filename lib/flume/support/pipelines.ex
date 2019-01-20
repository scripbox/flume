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

    get_pipelines()
    |> Enum.flat_map(fn pipeline ->
      pipeline_struct = Flume.Pipeline.new(pipeline)

      [
        worker(EventPipeline.Producer, [pipeline_struct], id: generate_id()),
        worker(EventPipeline.ProducerConsumer, [pipeline_struct], id: generate_id()),
        worker(EventPipeline.Consumer, [pipeline_struct], id: generate_id())
      ]
    end)
  end

  # Pipeline config
  # [%{name: "Pipeline1", queue: "default", rate_limit_count: 1000, rate_limit_scale: 5000}]
  defp get_pipelines, do: Flume.Config.get(:pipelines)

  defp generate_id do
    <<part1::32, part2::32>> = :crypto.strong_rand_bytes(8)
    "#{part1}#{part2}"
  end
end
