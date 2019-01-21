defmodule Flume.Pipeline.Event do
  alias Flume.Pipeline
  alias Flume.Pipeline.Event, as: EventPipeline
  alias Flume.Redis.Client, as: RedisClient
  alias Flume.Pipeline.Event.Stats, as: EventStats

  def init(%Pipeline{name: name} = pipeline) do
    # Register the pipeline in :ets
    EventStats.register(name)

    state = pipeline |> Map.merge(%{paused: paused_state(name)})
    upstream = producer_consumer_upstream_name(name)

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

  defp producer_consumer_upstream_name(pipeline_name), do: :"#{pipeline_name}_producer"

  def pause(pipeline_name) do
    RedisClient.set(paused_redis_key(pipeline_name), true)
    EventPipeline.ProducerConsumer.pause(pipeline_name)
  end

  def resume(pipeline_name) do
    RedisClient.del(paused_redis_key(pipeline_name))
    EventPipeline.ProducerConsumer.resume(pipeline_name)
  end

  def wait_for_idle_consumers(pipeline_name) do
    EventPipeline.Consumer.workers_count(pipeline_name)
    |> Kernel.!=(0)
    |> if do
      Process.sleep(5000)
      wait_for_idle_consumers(pipeline_name)
    end

    :ok
  end

  def stop(timeout) do
    Flume.Config.pipelines()
    |> Enum.map(& &1.name)
    |> Task.async_stream(
      EventPipeline.Producer,
      :stop,
      [timeout],
      timeout: timeout
    )
    |> Enum.to_list()
    |> Enum.uniq()
    |> case do
      [ok: :ok] ->
        Supervisor.stop(Flume.Supervisor)

      _ ->
        :error
    end
  end

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
