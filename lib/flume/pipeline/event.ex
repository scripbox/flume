defmodule Flume.Pipeline.Event do
  alias Flume.{Config, Instrumentation, Pipeline}
  alias Flume.Pipeline.Event, as: EventPipeline
  alias Flume.Redis.Client, as: RedisClient

  def attach_instrumentation(%Pipeline{name: name, queue: queue, instrument: true} = _pipeline) do
    instrumentation = Config.instrumentation()
    name_atom = String.to_atom(name)
    queue_atom = String.to_atom(queue)

    Instrumentation.attach_many(
      name_atom,
      [
        [name_atom, :worker, :duration],
        [name_atom, :worker, :job, :duration],
        [queue_atom, :enqueue, :payload_size],
        [queue_atom, :dequeue],
        [queue_atom, :dequeue, :latency],
        [queue_atom, :dequeue, :payload_size]
      ],
      fn event_name, event_value, metadata, config ->
        apply(
          instrumentation[:handler_module],
          instrumentation[:handler_function],
          [event_name, event_value, metadata, config]
        )
      end,
      instrumentation[:metadata]
    )
  end

  def attach_instrumentation(_), do: nil

  def paused_state(pipeline_name) do
    paused_redis_key(pipeline_name)
    |> RedisClient.get!()
    |> case do
      nil -> false
      value -> String.to_existing_atom(value)
    end
  end

  def pause(pipeline_name, temporary \\ true)

  def pause(pipeline_name, true) do
    EventPipeline.Producer.pause(pipeline_name)
  end

  def pause(pipeline_name, false) do
    RedisClient.set(paused_redis_key(pipeline_name), true)
    EventPipeline.Producer.pause(pipeline_name)
  end

  def resume(pipeline_name, temporary \\ true)

  def resume(pipeline_name, true) do
    EventPipeline.Producer.resume(pipeline_name)
  end

  def resume(pipeline_name, false) do
    RedisClient.del(paused_redis_key(pipeline_name))
    EventPipeline.Producer.resume(pipeline_name)
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
end
