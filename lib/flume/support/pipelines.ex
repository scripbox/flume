defmodule Flume.Support.Pipelines do
  @moduledoc """
  This module returns the pipelines and its children
  based on the configuration
  """

  alias Flume.Pipeline.Event, as: EventPipeline
  alias Flume.Redis.Client, as: RedisClient

  @default_rate_limit_count 1000
  @default_rate_limit_scale 5000

  # Public API
  def list do
    import Supervisor.Spec

    # initialize the :ets table to store pipeline stats
    EventPipeline.Stats.init()

    get_pipelines()
    |> Enum.flat_map(fn %{name: pipeline_name} = pipeline ->
      paused_fn = paused_fn(pipeline_name)

      [
        worker(EventPipeline.Producer, [producer_options(pipeline)], id: generate_id()),
        worker(
          EventPipeline.ProducerConsumer,
          [consumer_options(pipeline) |> Map.merge(%{paused_fn: paused_fn})],
          id: generate_id()
        ),
        worker(EventPipeline.Consumer, [consumer_options(pipeline)], id: generate_id())
      ]
    end)
  end

  def pause(pipeline_name) do
    RedisClient.set(paused_redis_key(pipeline_name), true)
    EventPipeline.ProducerConsumer.pause(pipeline_name)
  end

  def resume(pipeline_name) do
    RedisClient.del(paused_redis_key(pipeline_name))
    EventPipeline.ProducerConsumer.resume(pipeline_name)
  end

  # Private API

  # Pipeline config
  # [%{name: "Pipeline1", queue: "default", rate_limit_count: 1000, rate_limit_scale: 5000}]
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

  defp consumer_options(pipeline) do
    max_demand = parse_max_demand(pipeline)
    interval = parse_interval(pipeline)
    batch_size = parse_batch_size(pipeline)

    %{
      name: pipeline[:name],
      max_demand: max_demand,
      interval: interval,
      batch_size: batch_size
    }
  end

  defp parse_max_demand(%{rate_limit_count: rate_limit_count} = _pipeline) do
    rate_limit_count
    |> to_string()
    |> Integer.parse()
    |> case do
      {count, _} ->
        count

      # default max demand
      :error ->
        @default_rate_limit_count
    end
  end

  defp parse_max_demand(_pipeline), do: @default_rate_limit_count

  defp parse_interval(%{rate_limit_scale: rate_limit_scale} = _pipeline) do
    rate_limit_scale
    |> to_string()
    |> Integer.parse()
    |> case do
      {scale, _} ->
        scale

      # in milliseconds (5 seconds)
      :error ->
        @default_rate_limit_scale
    end
  end

  defp parse_interval(_pipeline), do: @default_rate_limit_scale

  defp parse_batch_size(%{batch_size: batch_size} = _pipeline) do
    batch_size
    |> to_string()
    |> Integer.parse()
    |> case do
      {batch_size, _} ->
        batch_size

      # disable batch processing (Default)
      :error ->
        nil
    end
  end

  defp parse_batch_size(_pipeline), do: nil

  defp paused_fn(pipeline_name) do
    fn ->
      case RedisClient.get!(paused_redis_key(pipeline_name)) do
        nil ->
          false

        value ->
          String.to_existing_atom(value)
      end
    end
  end

  defp paused_redis_key(pipeline_name) do
    "flume:pipeline:#{pipeline_name}:paused"
  end
end
