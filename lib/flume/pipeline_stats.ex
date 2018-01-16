defmodule Flume.PipelineStats do
  @moduledoc """
  This module will update an :ets table with the following information per pipeline:
    * Number of pending events
    * Number of finished events
    * Number of failed events
  """
  require Logger

  alias Flume.Redis.Client

  @stats_table :pipeline_stats
  @ets_options [:set, :named_table, :public, read_concurrency: false, write_concurrency: false, keypos: 1]
  @redis_namespace Flume.Config.get(:namespace)

  # Public API
  @doc """
  Initializes the ets table for the pipeline stats.
  """
  def init do
    :ets.new(@stats_table, @ets_options)
  end

  @doc """
  Returns the stats table name.
  """
  def ets_table_name do
    @stats_table
  end

  @doc """
  It inserts a default entry for the pipeline in ETS
  """
  def register(pipeline_name) do
    true = :ets.insert(@stats_table, new_entry(pipeline_name))
  end

  @doc """
  It outputs the current stats about each pipeline
  """
  def find(pipeline_name) do
    match =
      [{{pipeline_name, :"$1", :"$2", :"$3", :"$4", :"$5"}, [], [{{pipeline_name, :"$1", :"$2", :"$3", :"$4", :"$5"}}]}]

    [{_pipeline_name, pending, finished, failed, _, _}] = :ets.select(@stats_table, match)
    {:ok, pending, finished, failed}
  end

  # Updates the pipeline's pending events count by `count`
  def update(_attribute, _pipeline_name, 0), do: {:ok, 0}
  def update(:pending, pipeline_name, count) when count > 0 do
    pending = :ets.update_counter(@stats_table, pipeline_name, {2, count})
    {:ok, pending}
  end

  # Increments the pipeline's pending events count by 1
  def incr(:pending, pipeline_name) do
    pending = :ets.update_counter(@stats_table, pipeline_name, {2, 1})
    {:ok, pending}
  end
  # Increments the pipeline's finished events count by 1
  def incr(:finished, pipeline_name) do
    finished = :ets.update_counter(@stats_table, pipeline_name, {3, 1})
    {:ok, finished}
  end
  # Increments the pipeline's failed events count by 1
  def incr(:failed, pipeline_name) do
    failed = :ets.update_counter(@stats_table, pipeline_name, {4, 1})
    {:ok, failed}
  end

  # Decrements the pipline's pending events count by 1
  def decr(:pending, pipeline_name) do
    pending = :ets.update_counter(@stats_table, pipeline_name, {2, -1})
    {:ok, pending}
  end
  # Decrements the pipline's finished events count by 1
  def decr(:finished, pipeline_name) do
    finished = :ets.update_counter(@stats_table, pipeline_name, {3, -1})
    {:ok, finished}
  end
  # Decrements the pipline's failed events count by 1
  def decr(:failed, pipeline_name) do
    failed = :ets.update_counter(@stats_table, pipeline_name, {4, -1})
    {:ok, failed}
  end

  @doc """
  Persist pipeline stats to Redis.
  """
  def persist do
    cmds = Enum.reduce(stats(), [], fn {pipeline, _pending, finished, failed, last_finished, last_failed}, commands ->
      delta_finished = finished - last_finished
      delta_failed   = failed - last_failed
      :ets.update_counter(@stats_table, pipeline, [{5, delta_finished}, {6, delta_failed}])

      [redis_incrby(pipeline, :finished, delta_finished) | [redis_incrby(pipeline, :failed, delta_failed) | commands]]
    end)
    cmds |> Enum.reject(&(&1 == nil)) |> flush_to_redis!
  end

  # Private API
  defp flush_to_redis!([]), do: :ok
  defp flush_to_redis!(cmds) do
    case Client.pipeline(Flume.Redis, cmds) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp stats, do: :ets.tab2list(@stats_table)
  defp new_entry(pipeline_name), do: {pipeline_name, 0, 0, 0, 0, 0}

  defp redis_incrby(_, _, 0), do: nil
  defp redis_incrby(pipeline, attribute, count) do
    ["INCRBY", "#{@redis_namespace}:stat:#{attribute}:#{pipeline}", count]
  end
end
