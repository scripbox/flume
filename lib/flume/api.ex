defmodule Flume.API do
  @doc false
  defmacro __using__(_) do
    quote location: :keep do
      alias Flume.{Config, Pipeline}
      alias Flume.Queue.Manager

      def enqueue(
            queue,
            worker,
            function_name \\ :perform,
            args,
            opts \\ []
          ) do
        Manager.enqueue(
          namespace(),
          queue,
          worker,
          function_name,
          args,
          opts
        )
      end

      def bulk_enqueue(queue, jobs, opts \\ []) do
        Manager.bulk_enqueue(namespace(), queue, jobs, opts)
      end

      def enqueue_in(
            queue,
            time_in_seconds,
            worker,
            function_name \\ :perform,
            args,
            opts \\ []
          ) do
        Manager.enqueue_in(
          namespace(),
          queue,
          time_in_seconds,
          worker,
          function_name,
          args,
          opts
        )
      end

      def pause_all(temporary \\ true) do
        Config.pipeline_names() |> Enum.map(&pause(&1, temporary))
      end

      defp namespace, do: Config.namespace()

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
