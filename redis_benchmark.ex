defmodule RedisBenchmark do
  alias Flume.Config
  alias Flume.Queue.Manager

  @namespace Config.namespace()

  @default_poll_timeout 500
  @rate_limit_opts %{rate_limit_count: 50_000, rate_limit_scale: 1000}

  def start_enqueue_dequeue(dequeue_fn, count, queues, dequeue_batch, enqueue_concurrency) do
    clear_redis()
    jobs_queue_mapping = build_jobs_queue_mapping(count, queues)
    dequeue_tasks = start_dequeue(jobs_queue_mapping, dequeue_fn, dequeue_batch)
    enqueue(jobs_queue_mapping, enqueue_concurrency)
    Enum.each(dequeue_tasks, &Process.exit(&1, :normal))
    clear_redis()
  end

  def poll(function, poll_timeout) do
    function.()
    Process.sleep(poll_timeout)
    poll(function, poll_timeout)
  end

  defp start_dequeue(jobs_queue_mapping, dequeue_fn, dequeue_batch) do
    Enum.map(jobs_queue_mapping, fn {_jobs, queue} ->
      {:ok, pid} =
        start_poll_server(fn ->
          dequeue_fn.(queue, dequeue_batch)
        end)

      pid
    end)
  end

  defp enqueue(jobs_queue_mapping, concurrency) do
    enqueue_each = fn {jobs, queue} ->
      Enum.each(jobs, fn job ->
        {:ok, _} = Manager.enqueue(@namespace, queue, :worker, :perform, [job])
      end)
    end

    enqueue = fn {jobs, queue} ->
      chunks = Enum.split(jobs, concurrency) |> Tuple.to_list()

      Enum.map(chunks, fn chunk ->
        Task.async(fn -> enqueue_each.({chunk, queue}) end)
      end)
    end

    Enum.flat_map(jobs_queue_mapping, enqueue)
    |> Enum.each(&Task.await(&1, :infinity))
  end

  defp start_poll_server(function, poll_timeout \\ @default_poll_timeout) do
    Task.start_link(__MODULE__, :poll, [function, poll_timeout])
  end

  defp build_jobs_queue_mapping(count, queue_nos) do
    jobs = build_jobs(count)

    chunked = Enum.chunk_every(jobs, round(count / queue_nos))

    Enum.with_index(chunked)
    |> Enum.map(fn {jobs, idx} ->
      {jobs, "test:#{idx}"}
    end)
  end

  def new_bulk_dequeue(queue, batch) do
    {:ok, _jobs} =
      Manager.fetch_jobs_optimistic(
        @namespace,
        queue,
        batch,
        @rate_limit_opts
      )
  end

  def old_bulk_dequeue(queue, batch) do
    {:ok, _jobs} =
      Manager.fetch_jobs(
        @namespace,
        queue,
        batch,
        @rate_limit_opts
      )
  end

  defp build_jobs(nos), do: Enum.map(1..nos, &build_job/1)

  defp build_job(id), do: %{id: id}

  defp clear_redis do
    pool_size = Config.redis_pool_size()
    conn_key = :"#{Flume.Redis.Supervisor.redix_worker_prefix()}_#{pool_size - 1}"
    keys = Redix.command!(conn_key, ["KEYS", "#{Config.namespace()}:*"])
    Enum.map(keys, fn key -> Redix.command(conn_key, ["DEL", key]) end)
  end
end

count = 10_000_000
queues = 100
batch = 1000
enqueue_concurrency = 10000

Benchee.run(%{
  "old_enqueue_dequeue" => fn ->
    RedisBenchmark.start_enqueue_dequeue(
      &RedisBenchmark.old_bulk_dequeue/2,
      count,
      queues,
      batch,
      enqueue_concurrency
    )
  end,
  "new_enqueue_dequeue" => fn ->
    RedisBenchmark.start_enqueue_dequeue(
      &RedisBenchmark.new_bulk_dequeue/2,
      count,
      queues,
      batch,
      enqueue_concurrency
    )
  end
})