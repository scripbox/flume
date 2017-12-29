defmodule Flume.Queue do
  @doc false
  defmacro __using__(_) do
    quote location: :keep do
      alias Exq.Support.Config

      def enqueue(queue, worker, args) do
        GenServer.call(Flume.Queue.Server, {:enqueue, queue, worker, args})
      end

      def remove_job(queue, job) do
        GenServer.call(Flume.Queue.Server, {:remove_job, queue, job})
      end

      def fetch_jobs(queue, count) do
        GenServer.call(Flume.Queue.Server, {:fetch_jobs, queue, count})
      end

      def retry_or_fail_job(queue, job, retry_count, error) do
        GenServer.call(Flume.Queue.Server, {:retry_or_fail_job, queue, job, retry_count, error})
      end

      def retry_or_fail_job(queue, job, error) do
        GenServer.call(Flume.Queue.Server, {:retry_or_fail_job, queue, job, error})
      end
    end
  end
end
