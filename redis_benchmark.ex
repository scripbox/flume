defmodule RedisBenchmark do
  alias Flume.Config
  alias Flume.Queue.Manager

  @namespace Config.namespace()

  @rate_limit_opts %{rate_limit_count: 50_000, rate_limit_scale: 1000}

  @defaults [
    count: 100_000,
    queues: 20,
    dequeue_batch: 1000,
    enqueue_concurrency: 1000,
    # (count * pre_seed_multiplier) jobs get pre-seeded split into all queues
    pre_seed_multiplier: 2,
    dequeue_poll_timeout: 500
  ]

  def defaults, do: @defaults

  def start_enqueue_dequeue(
        enqueue_fn,
        dequeue_fn,
        jobs_queue_mapping,
        _opts = %{
          count: count,
          queues: queues,
          dequeue_batch: dequeue_batch,
          enqueue_concurrency: enqueue_concurrency,
          pre_seed_multiplier: pre_seed_multiplier,
          dequeue_poll_timeout: dequeue_poll_timeout
        }
      ) do
    dequeue_tasks =
      start_dequeue(jobs_queue_mapping, dequeue_fn, dequeue_batch, dequeue_poll_timeout)

    enqueue_fn.(jobs_queue_mapping, enqueue_concurrency)
    Enum.each(dequeue_tasks, &Process.exit(&1, :normal))
  end

  def enqueue(jobs_queue_mapping, concurrency) do
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

  def clear_redis do
    pool_size = Config.redis_pool_size()
    conn_key = :"#{Flume.Redis.Supervisor.redix_worker_prefix()}_#{pool_size - 1}"
    keys = Redix.command!(conn_key, ["KEYS", "#{Config.namespace()}:*"])
    Enum.map(keys, fn key -> Redix.command(conn_key, ["DEL", key]) end)
  end

  def poll(function, poll_timeout) do
    function.()
    Process.sleep(poll_timeout)
    poll(function, poll_timeout)
  end

  def build_jobs_queue_mapping(count, queue_nos) do
    jobs = build_jobs(count)

    chunked = Enum.chunk_every(jobs, round(count / queue_nos))

    Enum.with_index(chunked)
    |> Enum.map(fn {jobs, idx} ->
      {jobs, "test:#{idx}"}
    end)
  end

  def pre_seed_queues(jobs_queue_mapping, multiplier) do
    enqueue = fn {job_ids, queue} ->
      jobs = Enum.map(job_ids, fn id -> [:worker, :perform, [id]] end)
      {:ok, _} = Manager.bulk_enqueue(@namespace, queue, jobs)
    end

    Enum.each(1..multiplier, fn _ ->
      Enum.each(jobs_queue_mapping, enqueue)
    end)
  end

  defp start_dequeue(jobs_queue_mapping, dequeue_fn, dequeue_batch, dequeue_poll_timeout) do
    Enum.map(jobs_queue_mapping, fn {_jobs, queue} ->
      {:ok, pid} =
        start_poll_server(
          fn ->
            dequeue_fn.(queue, dequeue_batch)
          end,
          dequeue_poll_timeout
        )

      pid
    end)
  end

  defp start_poll_server(function, poll_timeout) do
    Task.start_link(__MODULE__, :poll, [function, poll_timeout])
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
end

user_options =
  OptionParser.parse(
    System.argv(),
    strict: [
      count: :integer,
      queues: :integer,
      dequeue_batch: :integer,
      enqueue_concurrency: :integer,
      pre_seed_multiplier: :integer,
      dequeue_poll_timeout: :integer
    ]
  )
  |> elem(0)

opts =
  RedisBenchmark.defaults()
  |> Keyword.merge(user_options)
  |> Enum.into(%{})

IO.inspect("Running benchmark with config")
IO.inspect(opts)

Benchee.run(
  %{
    "enqueue" => fn jobs_queue_mapping ->
      noop = fn _, _ -> [] end

      RedisBenchmark.start_enqueue_dequeue(
        &RedisBenchmark.enqueue/2,
        noop,
        jobs_queue_mapping,
        opts
      )
    end,
    "transactional_enqueue_dequeue" => fn jobs_queue_mapping ->
      RedisBenchmark.start_enqueue_dequeue(
        &RedisBenchmark.enqueue/2,
        &RedisBenchmark.old_bulk_dequeue/2,
        jobs_queue_mapping,
        opts
      )
    end,
    "optimistic_enqueue_dequeue" => fn jobs_queue_mapping ->
      RedisBenchmark.start_enqueue_dequeue(
        &RedisBenchmark.enqueue/2,
        &RedisBenchmark.new_bulk_dequeue/2,
        jobs_queue_mapping,
        opts
      )
    end
  },
  before_each: fn _ ->
    RedisBenchmark.clear_redis()
    jobs_queue_mapping = RedisBenchmark.build_jobs_queue_mapping(opts[:count], opts[:queues])
    RedisBenchmark.pre_seed_queues(jobs_queue_mapping, opts[:pre_seed_multiplier])
    jobs_queue_mapping
  end,
  after_each: fn _ -> RedisBenchmark.clear_redis() end
)
