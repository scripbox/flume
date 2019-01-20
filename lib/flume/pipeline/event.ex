defmodule Flume.Pipeline.Event do
  alias Flume.Pipeline
  alias Flume.Pipeline.Event, as: EventPipeline
  alias Flume.Redis.Client, as: RedisClient

  def init_producer_consumer(%Pipeline{name: name} = pipeline) do
    # Register the pipeline in :ets
    EventPipeline.Stats.register(name)

    state = pipeline |> Map.merge(%{paused: paused_state(name)})
    upstream = producer_consumer_upstream_name(name)

    {state, upstream}
  end

  def paused_state(pipeline_name) do
    paused_redis_key(pipeline_name)
    |> RedisClient.get!()
    |> case do
      nil -> :false

      value -> String.to_existing_atom(value)
    end
  end

  defp producer_consumer_upstream_name(pipeline_name), do: :"#{pipeline_name}_producer"

  def pause(pipeline_name) do
    RedisClient.set(paused_redis_key(pipeline_name), true)
    EventPipeline.ProducerConsumer.pause(pipeline_name)
  end

  def resume(pipeline_name) do
    RedisClient.del(paused_redis_key(pipeline_name))
    EventPipeline.ProducerConsumer.resume(pipeline_name)
  end

  defp paused_redis_key(pipeline_name), do: "flume:pipeline:#{pipeline_name}:paused"
end
