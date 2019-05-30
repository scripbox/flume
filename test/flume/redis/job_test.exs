defmodule JobTest do
  use TestWithRedis

  alias Flume.Config
  alias Flume.Redis.Job
  alias Flume.Support.Time, as: TimeExtension

  @namespace Config.namespace()
  @serialized_job "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1083fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"

  describe "enqueue/2" do
    test "enqueues a job to a queue" do
      assert {:ok, _} = Job.enqueue("#{@namespace}:test", @serialized_job)
    end
  end

  describe "bulk_enqueue/3" do
    test "enqueues array of jobs to a queue" do
      assert {:ok, [1, 2]} =
               Job.bulk_enqueue("#{@namespace}:test", [
                 @serialized_job,
                 "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1083fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[2]}"
               ])
    end
  end

  describe "bulk_dequeue/3" do
    test "dequeues multiple jobs and queues it to new list" do
      jobs = [
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1082fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1182fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1282fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1382fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1482fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1582fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1682fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1782fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1882fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}",
        "{\"class\":\"Elixir.Worker\",\"queue\":\"test\",\"jid\":\"1982fd87-2508-4eb4-8fba-2958584a60e3\",\"enqueued_at\":1514367662,\"args\":[1]}"
      ]

      Enum.map(jobs, fn job -> Job.enqueue("#{@namespace}:test", job) end)

      assert {:ok, jobs} ==
               Job.bulk_dequeue(
                 "#{@namespace}:test",
                 "#{@namespace}:processing:test",
                 10,
                 TimeExtension.time_to_score()
               )
    end

    test "dequeues multiple jobs from an empty queue" do
      assert {:ok, []} ==
               Job.bulk_dequeue(
                 "#{@namespace}:test",
                 "#{@namespace}:processing:test",
                 5,
                 TimeExtension.time_to_score()
               )
    end
  end

  describe "schedule_job/5" do
    test "schedules a job" do
      assert {:ok, 1} ==
               Job.schedule_job(
                 "#{@namespace}:test",
                 DateTime.utc_now() |> TimeExtension.unix_seconds(),
                 @serialized_job
               )
    end
  end

  describe "group_by_queue/1" do
    def build_scheduled_queue_and_job(scheduled_queue, queue, job),
      do: {scheduled_queue, queue, job}

    def build_scheduled_queue_and_jobs(scheduled_queue, queue, count),
      do: Enum.map(1..count, &build_scheduled_queue_and_job(scheduled_queue, queue, "#{&1}"))

    test "groups scheduled queues_and_jobs by queue" do
      group1 = build_scheduled_queue_and_jobs("s1", "q1", 10)
      group2 = build_scheduled_queue_and_jobs("s2", "q2", 10)
      group3 = build_scheduled_queue_and_jobs("s3", "q3", 10)
      grouped = Job.group_by_queue(group1 ++ group2 ++ group3)

      Enum.each(1..3, fn count ->
        queue = "q#{count}"
        assert grouped[queue] |> length == 10

        Enum.each(grouped[queue], fn {scheduled, _} ->
          assert scheduled == "s#{count}"
        end)
      end)
    end
  end

  describe "remove_job/3" do
    test "removes a job from a queue" do
      Job.enqueue("#{@namespace}:test", @serialized_job)

      assert 1 == Job.remove_job!("#{@namespace}:test", @serialized_job)
    end
  end

  describe "remove_retry_job/3" do
    test "removes a job from a retry queue" do
      Job.fail_job!("#{@namespace}:test", @serialized_job)

      assert 1 == Job.remove_scheduled_job!("#{@namespace}:test", @serialized_job)
    end
  end

  describe "fail_job/3" do
    test "adds a job to retry queue" do
      assert 1 == Job.fail_job!("#{@namespace}:test", @serialized_job)
    end
  end

  describe "fetch_all/2" do
    test "fetches all jobs from a list" do
      Job.enqueue("#{@namespace}:test", @serialized_job)

      assert [@serialized_job] == Job.fetch_all!("#{@namespace}:test")
    end
  end

  describe "scheduled_job/3" do
    test "returns scheduled jobs" do
      {:ok, _jid} =
        Job.schedule_job(
          "#{@namespace}:test",
          DateTime.utc_now() |> TimeExtension.unix_seconds(),
          @serialized_job
        )

      {:ok, jobs} =
        Job.scheduled_jobs(
          ["#{@namespace}:test"],
          TimeExtension.time_to_score()
        )

      assert [{"#{@namespace}:test", [@serialized_job]}] == jobs
    end
  end
end
