defmodule Flume.API do
  @doc false
  defmacro __using__(_) do
    quote location: :keep do
      alias Flume.{Config, Pipeline}
      alias Flume.Queue.Manager

      @namespace Config.namespace()

      def enqueue(queue, worker, function_name \\ :perform, args) do
        Manager.enqueue(@namespace, queue, worker, function_name, args)
      end

      def bulk_enqueue(queue, jobs) do
        Manager.bulk_enqueue(@namespace, queue, jobs)
      end

      def enqueue_in(queue, time_in_seconds, worker, function_name \\ :perform, args) do
        Manager.enqueue_in(@namespace, queue, time_in_seconds, worker, function_name, args)
      end

      def fetch_jobs(
            queue,
            count
          ) do
        Manager.fetch_jobs(@namespace, queue, count)
      end

      def fetch_jobs(
            queue,
            count,
            rate_limit_count,
            rate_limit_scale
          ) do
        Manager.fetch_jobs(@namespace, queue, count, rate_limit_count, rate_limit_scale)
      end

      def retry_or_fail_job(queue, job, error) do
        Manager.retry_or_fail_job(@namespace, queue, job, error)
      end

      def fail_job(job, error) do
        Manager.fail_job(@namespace, job, error)
      end

      def remove_job(queue, job) do
        Manager.remove_job(@namespace, queue, job)
      end

      def remove_retry(job) do
        Manager.remove_retry(@namespace, job)
      end

      def remove_processing(queue, job) do
        Manager.remove_processing(@namespace, queue, job)
      end

      def pause_all(temporary \\ true) do
        Config.pipeline_names() |> Enum.map(&pause(&1, temporary))
      end

      defdelegate pause(pipeline_name, temporary \\ true), to: Pipeline.Event

      defdelegate resume(pipeline_name, temporary \\ true), to: Pipeline.Event

      def pending_jobs_count(pipeline_names \\ Config.pipeline_names()) do
        Pipeline.Event.pending_workers_count(pipeline_names) +
          Pipeline.SystemEvent.pending_workers_count()
      end
    end
  end
end
