defmodule Flume.Support.Pipelines do
  @moduledoc """
  This module returns the pipelines and its children
  based on the configuration
  """

  alias Flume.Pipeline.Event, as: EventPipeline

  # Public API
  def list do
    import Supervisor.Spec

    get_pipelines()
    |> Enum.flat_map(fn pipeline ->
      [
        worker(EventPipeline.Producer, [pipeline_options(pipeline)], id: generate_id()),
        worker(EventPipeline.ProducerConsumer, [pipeline_options(pipeline)], id: generate_id()),
        worker(EventPipeline.Consumer, [pipeline_options(pipeline)], id: generate_id())
      ]
    end)
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

  defp pipeline_options(pipeline) do
    max_demand =
      case Integer.parse(to_string(pipeline[:rate_limit_count])) do
        {count, _} ->
          count

        # default max demand
        :error ->
          1000
      end

    interval =
      case Integer.parse(to_string(pipeline[:rate_limit_scale])) do
        {scale, _} ->
          scale

        # in milliseconds
        :error ->
          5000
      end

    %{
      name: pipeline[:name],
      queue: pipeline[:queue],
      max_demand: max_demand,
      interval: interval
    }
  end
end
