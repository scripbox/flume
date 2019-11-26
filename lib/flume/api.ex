defmodule Flume.API do
  @doc false
  defmacro __using__(_) do
    quote location: :keep do
      alias Flume.{Config, Pipeline}
      alias Flume.Queue.Manager

      import Flume.Queue.API

      def pause_all(temporary \\ true) do
        Config.pipeline_names() |> Enum.map(&pause(&1, temporary))
      end

      defdelegate pause(pipeline_name, temporary \\ true), to: Pipeline.Event

      defdelegate resume(pipeline_name, temporary \\ true), to: Pipeline.Event

      def pending_jobs_count(pipeline_names \\ Config.pipeline_names()) do
        Pipeline.Event.pending_workers_count(pipeline_names) +
          Pipeline.SystemEvent.pending_workers_count()
      end

      def worker_context, do: Pipeline.Context.get()
    end
  end
end
