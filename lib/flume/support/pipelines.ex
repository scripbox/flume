defmodule Flume.Support.Pipelines do
  @moduledoc """
  This module returns the pipelines and its children
  based on the configuration
  """

  # Public API
  def list do
    import Supervisor.Spec

    pipelines = get_pipelines()

    Enum.map(pipelines, fn(pipeline) ->
      [
        worker(Flume.Producer, [%{name: pipeline.name, queue: pipeline.queue}], id: generate_id()),
        supervisor(Flume.ConsumerSupervisor, [%{name: pipeline.name, concurrency: pipeline.concurrency}], id: generate_id())
      ]
    end) |> List.flatten
  end

  # Private API

  # Pipeline config
  # [%{name: "Pipeline1", queue: "default", concurrency: 100}]
  defp get_pipelines do
    Flume.Config.get(:pipelines)
  end

  defp generate_id do
    <<part1::32, part2::32>> = :crypto.strong_rand_bytes(8)
    "#{part1}#{part2}"
  end
end
