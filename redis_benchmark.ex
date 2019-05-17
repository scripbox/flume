defmodule RedisBenchmark do
  alias Flume.Config
  alias Flume.Redis.{Job, Client}
  alias Flume.Support.Time, as: TimeExtension

  @namespace Config.namespace()

  def old_enqueue_dequeue(count) do
    jobs_queues_mapping = build_jobs_queues_mapping(count)
    start_enqueue(jobs_queues_mapping)

    _dequeue_tasks =
      Enum.map(jobs_queues_mapping, fn {_jobs, queue} ->
        {:ok, pid} =
          start_poll_server(fn ->
            old_bulk_dequeue(queue, 10)
          end)

        pid
      end)
  end

  defp start_enqueue(jobs_queues_mapping) do
    enqueue_each = fn {jobs, queue} ->
      Enum.each(jobs, fn job ->
        {:ok, _} = Job.enqueue(queue, job)
      end)
    end

    Enum.map(jobs_queues_mapping, fn {jobs, queue} ->
      Task.async(fn ->
        enqueue_each.({jobs, queue})
      end)
    end)
    |> Enum.each(&Task.await(&1, :infinity))
  end

  def start_poll_server(function, poll_timeout \\ 500) do
    Task.start_link(__MODULE__, :poll, [function, poll_timeout])
  end

  def poll(function, poll_timeout) do
    function.()
    Process.sleep(poll_timeout)
    poll(function, poll_timeout)
  end

  defp build_jobs_queues_mapping(count, queue_nos \\ 100) do
    jobs = build_jobs(count)

    chunked = Enum.chunk_every(jobs, round(count / queue_nos))

    Enum.with_index(chunked)
    |> Enum.map(fn {jobs, idx} ->
      {jobs, "#{@namespace}:test:#{idx}"}
    end)
  end

  defp old_bulk_dequeue(queue, batch) do
    no_throttling = 9_999_999_999_999

    {:ok, _jobs} =
      Job.bulk_dequeue(
        queue,
        "#{queue}:processing",
        "#{queue}:limit",
        batch,
        no_throttling,
        TimeExtension.time_to_score(),
        TimeExtension.time_to_score()
      )
  end

  def build_jobs(nos), do: Enum.map(1..nos, &build_job/1)

  def build_job(id),
    do:
      "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"#{id}82fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[{ \"name\": \"investment_scheduled_sip_event\", \"profile_id\": \"cf70b483-c7ca-4d90-8078-9235eb0fb826\", \"event_type_id\": \"8aa37730-92a9-4473-92f8-5833d7540bee\", \"communication_mode\": \"email\", \"status\": \"clicked\", \"created_at\": \"2019-05-17 14:13:20.662795Z\" } ]}"

  def clear_redis do
    pool_size = Config.redis_pool_size()
    conn_key = :"#{Flume.Redis.Supervisor.redix_worker_prefix()}_#{pool_size - 1}"
    keys = Redix.command!(conn_key, ["KEYS", "#{Config.namespace()}:*"])
    Enum.map(keys, fn key -> Redix.command(conn_key, ["DEL", key]) end)
  end
end

RedisBenchmark.clear_redis()

count = 10_000_000

Benchee.run(%{
  "old_enqueue_dequeue" => fn -> RedisBenchmark.old_enqueue_dequeue(count) end
})

RedisBenchmark.clear_redis()
