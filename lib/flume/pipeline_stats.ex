defmodule Flume.PipelineStats do
  @moduledoc """
  This module will update an :ets table with the following information per pipeline:
    * Number of pending events
  """
  require Logger

  @stats_table :pipeline_stats
  @ets_options [:set, :named_table, :public, read_concurrency: false, write_concurrency: false, keypos: 1]

  # Public API
  @doc """
  Initializes the ets table for the pipeline stats.
  """
  def init do
    :ets.new(@stats_table, @ets_options)
  end

  @doc """
  It inserts a default entry for the pipeline in ETS
  """
  def register(pipeline_name) do
    true = :ets.insert(@stats_table, new_entry(pipeline_name))
  end

  @doc """
  It outputs the current stats about each queue and `total` and search for a `prefix` if provided
  """
  def find(pipeline_name) do
    match =
      [{{pipeline_name, :"$1"}, [], [{{pipeline_name, :"$1"}}]}]

    [{_pipeline_name, pending}] = :ets.select(@stats_table, match)
    {:ok, pending}
  end

  # Increments the pipeline's pending events count by `count`
  def incr(:pending, _pipeline_name, 0), do: {:ok, 0}
  def incr(:pending, pipeline_name, count) when count > 0 do
    pending = :ets.update_counter(@stats_table, pipeline_name, count, {0, 0})
    {:ok, pending}
  end

  # Decrements the pipline's pending events count by 1
  def decr(:pending, pipeline_name) do
    pending = :ets.update_counter(@stats_table, pipeline_name, -1, {0, 0})
    {:ok, pending}
  end

  # Private API
  defp new_entry(pipeline_name), do: {pipeline_name, 0}
end
