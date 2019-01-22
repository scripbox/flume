defmodule Flume.API do
  @doc false
  defmacro __using__(_) do
    quote location: :keep do
      alias Flume.Config
      alias Flume.Queue.Manager
      alias Flume.Pipeline.Event, as: EventPipeline

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

      def fetch_jobs(queue, count) do
        Manager.fetch_jobs(@namespace, queue, count)
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

      def remove_backup(queue, job) do
        Manager.remove_backup(@namespace, queue, job)
      end

      defdelegate pause(pipeline_name), to: EventPipeline

      defdelegate resume(pipeline_name), to: EventPipeline

      defdelegate stop(timeout \\ :infinity), to: EventPipeline
    end
  end
end
