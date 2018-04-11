defmodule Flume.Queue do
  @doc false
  defmacro __using__(_) do
    quote location: :keep do
      alias Flume.Config
      alias Flume.Queue.Server

      def enqueue(queue, worker, args) do
        server_call(&Server.enqueue(&1, queue, worker, :perform, args))
      end

      def enqueue(queue, worker, function_name, args) do
        server_call(&Server.enqueue(&1, queue, worker, function_name, args))
      end

      def enqueue_in(queue, time_in_seconds, worker, args) do
        server_call(&Server.enqueue_in(&1, queue, time_in_seconds, worker, :perform, args))
      end

      def enqueue_in(queue, time_in_seconds, worker, function_name, args) do
        server_call(&Server.enqueue_in(&1, queue, time_in_seconds, worker, function_name, args))
      end

      def fetch_jobs(queue, count) do
        server_call(&Server.fetch_jobs(&1, queue, count))
      end

      def retry_or_fail_job(queue, job, error) do
        server_call(&Server.retry_or_fail_job(&1, queue, job, error))
      end

      def fail_job(job, error) do
        server_call(&Server.fail_job(&1, job, error))
      end

      def remove_job(queue, job) do
        server_call(&Server.remove_job(&1, queue, job))
      end

      def remove_retry(job) do
        server_call(&Server.remove_retry(&1, job))
      end

      def remove_backup(queue, job) do
        server_call(&Server.remove_backup(&1, queue, job))
      end

      def remove_backup_jobs(jobs) do
        server_call(&Server.remove_backup_jobs(&1, jobs))
      end

      defp server_call(func) do
        Task.async(fn ->
          :poolboy.transaction(
            Flume.queue_server_pool_name(),
            &func.(&1),
            Config.get(:server_timeout)
          )
        end)
        |> Task.await()
      end
    end
  end
end
