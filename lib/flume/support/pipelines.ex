defmodule Flume.Support.Pipelines do
  @moduledoc """
  This module returns the pipelines and its children
  based on the configuration
  """

  alias Flume.Pipeline.Event, as: EventPipeline
  alias Flume.Config, as: FlumeConfig

  @doc false
  def list do
    import Supervisor.Spec

    Flume.Config.pipelines()
    |> Enum.flat_map(fn pipeline ->
      pipeline_struct = Flume.Pipeline.new(pipeline)
      scheduler_options = FlumeConfig.scheduler_opts() ++ [queue: pipeline.queue]
      EventPipeline.attach_instrumentation(pipeline_struct)

      [
        worker(EventPipeline.Producer, [pipeline_struct], id: generate_id()),
        worker(EventPipeline.ProducerConsumer, [pipeline_struct], id: generate_id()),
        worker(EventPipeline.Consumer, [pipeline_struct], id: generate_id()),
        worker(Flume.Queue.BackupScheduler, [scheduler_options], id: generate_id())
      ]
    end)
  end

  defp generate_id do
    <<part1::32, part2::32>> = :crypto.strong_rand_bytes(8)
    "#{part1}#{part2}"
  end
end
