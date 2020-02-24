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
        [name_atom, :worker],
        [name_atom, :worker, :job],
        [queue_atom, :enqueue],
        [queue_atom, :dequeue]
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

  def pause(pipeline_name, opts) do
    unless opts[:temporary] do
      RedisClient.set(paused_redis_key(pipeline_name), true)
    end

    EventPipeline.Producer.pause(pipeline_name, opts[:async], opts[:timeout])
  end

  def resume(pipeline_name, opts) do
    unless opts[:temporary] do
      RedisClient.del(paused_redis_key(pipeline_name))
    end

    EventPipeline.Producer.resume(pipeline_name, opts[:async], opts[:timeout])
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

  defp paused_redis_key(pipeline_name),
    do: "#{Config.namespace()}:pipeline:#{pipeline_name}:paused"
end
