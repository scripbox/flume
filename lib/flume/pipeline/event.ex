defmodule Flume.Pipeline.Event do
  alias Flume.Pipeline
  alias Flume.Pipeline.Event, as: EventPipeline
  alias Flume.Redis.Client, as: RedisClient
  alias Flume.Pipeline.Event.Stats, as: EventStats

  @pending :pending
  @failed :failed
  @processed :processed

  def init_producer_consumer(%Pipeline{name: name} = pipeline) do
    # Register the pipeline in :ets
    EventStats.register(name)

    state = pipeline |> Map.merge(%{paused: paused_state(name)})
    upstream = producer_consumer_upstream_name(name)

    {state, upstream}
  end

  def paused_state(pipeline_name) do
    paused_redis_key(pipeline_name)
    |> RedisClient.get!()
    |> case do
      nil -> false
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

  def completed(pipeline_name, count \\ 1) do
    # decrements the :pending events count
    {:ok, _pending} = EventStats.decr(@pending, pipeline_name, count)
  end

  def failed(pipeline_name, count \\ 1) do
    # increments the :failed events count
    {:ok, _failed} = EventStats.incr(@failed, pipeline_name, count)
  end

  def processed(pipeline_name, count \\ 1) do
    # increments the :processed events count
    {:ok, _processed} = EventStats.incr(@processed, pipeline_name, count)
  end
end
