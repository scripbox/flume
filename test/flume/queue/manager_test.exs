defmodule Flume.Queue.ManagerTest do
  use Flume.TestWithRedis

  alias Flume.Queue.Manager
  alias Flume.Support.Time
  alias Flume.{Config, Event, JobFactory}
  alias Flume.Redis.{Client, Job, SortedSet}

  @namespace Config.namespace()

  def max_time_range do
    Flume.Queue.Backoff.calc_next_backoff(1)
    |> Flume.Support.Time.offset_from_now()
    |> Flume.Support.Time.time_to_score()
  end

  describe "enqueue/5" do
    test "enqueues a job into a queue" do
      assert {:ok, _} = Manager.enqueue(@namespace, "test", Worker, "process", [1])
    end
  end

  describe "bulk_enqueue/3" do
    test "enqueues array of jobs into a queue" do
      assert {:ok, 2} =
               Manager.bulk_enqueue(@namespace, "test", [
                 [Worker, "process", [1]],
                 [Worker, "process", [2]]
               ])
    end
  end

  describe "enqueue_in/6" do
    test "enqueues a job at a scheduled time" do
      assert {:ok, _} = Manager.enqueue_in(@namespace, "test", 10, Worker, "process", [1])
    end
  end

  describe "fetch_jobs/5" do
    test "fetch multiple jobs and queues it to a new list" do
      jobs = JobFactory.generate_jobs("Elixir.Worker", 10)

      Job.bulk_enqueue("#{@namespace}:queue:test", jobs)

      assert {:ok, jobs} ==
               Manager.fetch_jobs(@namespace, "test", 10, %{
                 rate_limit_count: 1000,
                 rate_limit_scale: 500
               })

      assert 10 == Client.zcount!("#{@namespace}:queue:limit:test")
    end

    test "maintains rate-limit for a given key" do
      jobs = JobFactory.generate_jobs("Elixir.Worker", 10)

      Job.bulk_enqueue("#{@namespace}:queue:test", jobs)

      assert {:ok, jobs} ==
               Manager.fetch_jobs(
                 @namespace,
                 "test",
                 10,
                 %{rate_limit_count: 1000, rate_limit_scale: 500, rate_limit_key: "test"}
               )

      assert 10 == Client.zcount!("#{@namespace}:limit:test")
      assert 0 == Client.zcount!("#{@namespace}:queue:limit:test")
    end

    test "dequeues multiple jobs from an empty queue" do
      assert {:ok, []} ==
               Manager.fetch_jobs(@namespace, "test", 5, %{
                 rate_limit_count: 1000,
                 rate_limit_scale: 500
               })
    end
  end

  describe "Concurrent fetches" do
    test "fetches unique jobs" do
      count = 2
      jobs = JobFactory.generate_jobs("Elixir.Worker", count)

      Job.bulk_enqueue("#{@namespace}:queue:test", jobs)

      results =
        Enum.map(1..count, fn _ ->
          Task.async(fn ->
            {:ok, jobs} =
              Manager.fetch_jobs(
                @namespace,
                "test",
                1,
                %{rate_limit_count: count, rate_limit_scale: 50000}
              )

            jobs
          end)
        end)
        |> Enum.flat_map(&Task.await/1)

      assert 1 == length(results)
      assert 1 == Client.zcount!("#{@namespace}:queue:limit:test")
    end
  end

  describe "retry_or_fail_job/4" do
    test "adds job to retry queue by incrementing count" do
      job =
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958522a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"

      SortedSet.add(
        "#{@namespace}:queue:processing:test",
        DateTime.utc_now() |> Time.time_to_score(),
        job
      )

      Manager.retry_or_fail_job(@namespace, "test", job, "Failed")

      # make sure the job is removed from the backup queue
      assert [] = Client.zrange!("#{@namespace}:queue:processing:test")

      {:ok, [{"#{@namespace}:retry", [retried_job]}]} =
        Job.scheduled_jobs(["#{@namespace}:retry"], max_time_range())

      retried_job = Flume.Event.decode!(retried_job)
      assert retried_job.jid == "1082fd87-2508-4eb4-8fba-2958522a60e3"
    end

    test "adds job to dead queue if count exceeds max retries" do
      job =
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2959994a60e3\",\"enqueued_at\":1514367662,\"args\":[1], \"retry_count\": 0}"

      SortedSet.add(
        "#{@namespace}:queue:processing:test",
        DateTime.utc_now() |> Time.time_to_score(),
        job
      )

      Manager.retry_or_fail_job(@namespace, "test", job, "Failed")

      # make sure the job is removed from the backup queue
      assert [] = Client.zrange!("#{@namespace}:queue:processing:test")

      Enum.map(1..Config.max_retries(), fn _retry_count ->
        {:ok, [{"#{@namespace}:retry", [job_to_retry]}]} =
          Job.scheduled_jobs(["#{@namespace}:retry"], max_time_range())

        Manager.retry_or_fail_job(@namespace, "test", job_to_retry, "Failed")
      end)

      # job will not be pushed to the retry queue
      assert {:ok, [{"#{@namespace}:retry", []}]} ==
               Job.scheduled_jobs(["#{@namespace}:retry"], max_time_range())

      {:ok, [{"#{@namespace}:dead", [job_in_dead_queue]}]} =
        Job.scheduled_jobs(["#{@namespace}:dead"], max_time_range())

      job_in_dead_queue = Flume.Event.decode!(job_in_dead_queue)
      assert job_in_dead_queue.jid == "1082fd87-2508-4eb4-8fba-2959994a60e3"
    end
  end

  describe "remove_retry/3" do
    test "remove job from a retry queue" do
      queue = "#{@namespace}:retry"

      job =
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"

      Job.schedule_job(queue, DateTime.utc_now() |> Time.unix_seconds(), job)

      assert {:ok, 1} == Manager.remove_retry(@namespace, job)
      assert {:ok, [{^queue, []}]} = Job.scheduled_jobs([queue], max_time_range())
    end
  end

  describe "remove_processing/3" do
    test "removes a job from processing sorted-set" do
      serialized_job =
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1084fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"

      Job.schedule_job(
        "#{@namespace}:queue:processing:test",
        Flume.Support.Time.unix_seconds(),
        serialized_job
      )

      assert {:ok, 1} == Manager.remove_processing(@namespace, "test", serialized_job)
    end
  end

  describe "remove_and_enqueue_scheduled_jobs/3" do
    test "remove and enqueue scheduled jobs" do
      Job.schedule_job(
        "#{@namespace}:scheduled",
        DateTime.utc_now() |> Time.unix_seconds(),
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      )

      Job.schedule_job(
        "#{@namespace}:retry",
        DateTime.utc_now() |> Time.unix_seconds(),
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd97-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      )

      queue = "#{@namespace}:queue:test"

      assert {:ok, 2} ==
               Manager.remove_and_enqueue_scheduled_jobs(
                 @namespace,
                 Time.time_to_score()
               )

      jobs = [
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd97-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      ]

      assert jobs == Job.fetch_all!(queue)
    end
  end

  describe "enqueue_processing_jobs/2" do
    test "enqueues jobs to main queue if job's score in processing sorted-set is less than the current-score" do
      job =
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"

      SortedSet.add(
        "#{@namespace}:queue:processing:test",
        DateTime.utc_now() |> Time.time_to_score(),
        job
      )

      Client.query!(["SCRIPT", "FLUSH"])
      assert {:ok, _} = Manager.enqueue_processing_jobs(@namespace, DateTime.utc_now(), "test", 1)

      assert [] = Client.zrange!("#{@namespace}:queue:processing:test")

      assert match?(
               %Event{jid: "1082fd87-2508-4eb4-8fba-2958584a60e3", enqueued_at: 1_514_367_662},
               Client.lrange!("#{@namespace}:queue:test") |> List.first() |> Event.decode!()
             )
    end

    test "enqueues only 1 job to main queue if job's score in processing sorted-set is less than the current-score" do
      job_1 =
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test_limit\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"

      SortedSet.add(
        "#{@namespace}:queue:processing:test_limit",
        DateTime.utc_now() |> Time.time_to_score(),
        job_1
      )

      job_2 =
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test_limit\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367771,\"args\":[1]}"

      SortedSet.add(
        "#{@namespace}:queue:processing:test_limit",
        Time.offset_from_now(1000) |> Time.time_to_score(),
        job_2
      )

      Manager.enqueue_processing_jobs(@namespace, DateTime.utc_now(), "test_limit", 1)

      assert [^job_2] = Client.zrange!("#{@namespace}:queue:processing:test_limit")

      [event] = Client.lrange!("#{@namespace}:queue:test_limit")

      assert match?(
               %Event{jid: "1082fd87-2508-4eb4-8fba-2958584a60e3", enqueued_at: 1_514_367_662},
               Event.decode!(event)
             )
    end
  end

  describe "job_counts/2" do
    test "returns counts of the requested queues" do
      jobs = JobFactory.generate_jobs("Elixir.Worker", 10)

      [
        queue_1,
        queue_2,
        _
      ] = Enum.map(1..3, &"test_#{&1}")

      Enum.map(1..2, &Job.bulk_enqueue("#{@namespace}:queue:test_#{&1}", jobs))

      assert {
               :ok,
               [
                 {queue_1, 10},
                 {queue_2, 10},
                 {"unknown-queue", 0}
               ]
             } == Manager.job_counts(@namespace, [queue_1, queue_2, "unknown-queue"])
    end
  end
end
