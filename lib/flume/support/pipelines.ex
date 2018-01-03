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
        worker(Flume.Producer, [producer_options(pipeline)], id: generate_id()),
        supervisor(Flume.ConsumerSupervisor, [supervisor_options(pipeline)], id: generate_id())
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

  defp producer_options(pipeline) do
    %{
      name: pipeline[:name],
      queue: pipeline[:queue]
    }
  end

  defp supervisor_options(pipeline) do
    %{
      name: pipeline[:name],
      concurrency: pipeline[:concurrency],
      rate_limit_count: pipeline[:rate_limit_count],
      rate_limit_scale: pipeline[:rate_limit_scale]
    }
  end
end
