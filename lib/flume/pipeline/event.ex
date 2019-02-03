defmodule Flume.Pipeline.Event do
  alias Flume.{Config, Instrumentation, Pipeline}
  alias Flume.Pipeline.Event, as: EventPipeline
  alias Flume.Redis.Client, as: RedisClient
  alias Flume.Pipeline.Event.Stats, as: EventStats

  def init(%Pipeline{name: name, instrument: true} = pipeline) do
    name_atom = String.to_atom(name)
    Instrumentation.attach_many(
      name_atom,
      [
        [name_atom, :worker, :duration],
        [name_atom, :worker, :job, :duration]
      ],
      Config.instrumentation()[:handler_function],
      Config.instrumentation()[:metadata]
    )

    do_init(pipeline)
  end

  def init(%Pipeline{name: _name} = pipeline), do: do_init(pipeline)

  defp do_init(%Pipeline{name: name} = pipeline) do
    # Register the pipeline in :ets
    EventStats.register(name)

    state = pipeline |> Map.merge(%{paused: paused_state(name)})
    upstream = producer_consumer_process_name(name)

    {state, upstream}
  end

  defp paused_state(pipeline_name) do
    paused_redis_key(pipeline_name)
    |> RedisClient.get!()
    |> case do
      nil -> false
      value -> String.to_existing_atom(value)
    end
  end

  defp producer_consumer_process_name(pipeline_name), do: :"#{pipeline_name}_producer"

  def pause(pipeline_name, temporary \\ true)

  def pause(pipeline_name, true) do
    EventPipeline.ProducerConsumer.pause(pipeline_name)
  end

  def pause(pipeline_name, false) do
    RedisClient.set(paused_redis_key(pipeline_name), true)
    EventPipeline.ProducerConsumer.pause(pipeline_name)
  end

  def resume(pipeline_name, temporary \\ true)

  def resume(pipeline_name, true) do
    EventPipeline.ProducerConsumer.resume(pipeline_name)
  end

  def resume(pipeline_name, false) do
    RedisClient.del(paused_redis_key(pipeline_name))
    EventPipeline.ProducerConsumer.resume(pipeline_name)
  end

  def pending_workers_count(pipeline_names \\ Flume.Config.pipeline_names()) do
    pipeline_names
    |> Enum.map(fn pipeline_name ->
      consumer_supervisor_process_name(pipeline_name)
      |> EventPipeline.Consumer.workers_count()
    end)
    |> Enum.sum()
  end

  defp consumer_supervisor_process_name(pipeline_name),
    do: :"#{pipeline_name}_consumer_supervisor"

  defp paused_redis_key(pipeline_name), do: "flume:pipeline:#{pipeline_name}:paused"

  def update_completed(pipeline_name, count \\ 1) do
    # decrements the :pending events count
    {:ok, _pending} = EventStats.decr(:pending, pipeline_name, count)
  end

  def update_failed(pipeline_name, count \\ 1) do
    # increments the :failed events count
    {:ok, _failed} = EventStats.incr(:failed, pipeline_name, count)
  end

  def update_processed(pipeline_name, count \\ 1) do
    # increments the :processed events count
    {:ok, _processed} = EventStats.incr(:processed, pipeline_name, count)
  end
end
