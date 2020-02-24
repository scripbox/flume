defmodule Flume.API do
  @doc false
  defmacro __using__(_) do
    quote location: :keep do
      alias Flume.{Config, Pipeline}
      alias Flume.Queue.Manager

      def bulk_enqueue(queue, jobs) do
        apply(Flume.Config.queue_api_module(), :bulk_enqueue, [queue, jobs])
      end

      def bulk_enqueue(queue, jobs, opts) do
        apply(Flume.Config.queue_api_module(), :bulk_enqueue, [queue, jobs, opts])
      end

      def enqueue(queue, worker, args) do
        apply(Flume.Config.queue_api_module(), :enqueue, [queue, worker, args])
      end

      def enqueue(queue, worker, function_name, args) do
        apply(Flume.Config.queue_api_module(), :enqueue, [queue, worker, function_name, args])
      end

      def enqueue(queue, worker, function_name, args, opts) do
        apply(Flume.Config.queue_api_module(), :enqueue, [
          queue,
          worker,
          function_name,
          args,
          opts
        ])
      end

      def enqueue_in(queue, time_in_seconds, worker, args) do
        apply(Flume.Config.queue_api_module(), :enqueue_in, [queue, time_in_seconds, worker, args])
      end

      def enqueue_in(queue, time_in_seconds, worker, function_name, args) do
        apply(Flume.Config.queue_api_module(), :enqueue_in, [
          queue,
          time_in_seconds,
          worker,
          function_name,
          args
        ])
      end

      def enqueue_in(queue, time_in_seconds, worker, function_name, args, opts) do
        apply(Flume.Config.queue_api_module(), :enqueue_in, [
          queue,
          time_in_seconds,
          worker,
          function_name,
          args,
          opts
        ])
      end

      @spec pause(Flume.Pipeline.Control.Options.option_spec()) :: list(:ok)
      def pause_all(options \\ []) do
        Config.pipeline_names() |> Enum.map(&pause(&1, options))
      end

      @spec pause(Flume.Pipeline.Control.Options.option_spec()) :: list(:ok)
      def resume_all(options \\ []) do
        Config.pipeline_names() |> Enum.map(&resume(&1, options))
      end

      @spec pause(String.t() | atom(), Flume.Pipeline.Control.Options.option_spec()) :: :ok
      defdelegate pause(pipeline_name, options \\ []), to: Pipeline

      @spec resume(String.t() | atom(), Flume.Pipeline.Control.Options.option_spec()) :: :ok
      defdelegate resume(pipeline_name, options \\ []), to: Pipeline

      defdelegate pipelines, to: Flume.Config

      def pending_jobs_count(pipeline_names \\ Config.pipeline_names()) do
        Pipeline.Event.pending_workers_count(pipeline_names) +
          Pipeline.SystemEvent.pending_workers_count()
      end

      def worker_context, do: Pipeline.Context.get()

      @doc """
      Returns count of jobs in the pipeline which are yet to be processed.

      ## Examples
      ```
      Flume.API.job_counts(["queue-1", "queue-2"])
      {:ok, [2, 3]}

      Flume.API.job_counts(["queue-1", "not-a-queue-name"])
      {:ok, [2, 0]}
      ```
      """
      @spec job_counts(nonempty_list(binary)) ::
              {:ok, nonempty_list(Redix.Protocol.redis_value())}
              | {:error, atom | Redix.Error.t() | Redix.ConnectionError.t()}
      def job_counts(queues), do: Manager.job_counts(namespace(), queues)

      defp namespace, do: Config.namespace()
    end
  end
end
