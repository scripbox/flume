defmodule Flume.Queue.ServerTest do
  use TestWithRedis, async: true

  import Flume.Queue.Server

  describe "enqueue/5" do
    test "timeout on enqueue" do
      pid = spawn_link(fn -> :timer.sleep(5000) end)

      assert :timeout == enqueue(pid, nil, nil, nil, nil)
    end
  end

  describe "enqueue_in/6" do
    test "timeout on enqueue_in" do
      pid = spawn_link(fn -> :timer.sleep(5000) end)

      assert :timeout == enqueue_in(pid, nil, nil, nil, nil, nil)
    end
  end

  describe "fetch_jobs/3" do
    test "timeout on fetch_jobs" do
      pid = spawn_link(fn -> :timer.sleep(5000) end)

      assert :timeout == fetch_jobs(pid, nil, nil)
    end
  end

  describe "retry_or_fail_job/4" do
    test "timeout on retry_or_fail_job" do
      pid = spawn_link(fn -> :timer.sleep(5000) end)

      assert :timeout == retry_or_fail_job(pid, nil, nil, nil)
    end
  end

  describe "fail_job/3" do
    test "timeout on fail_job" do
      pid = spawn_link(fn -> :timer.sleep(5000) end)

      assert :timeout == fail_job(pid, nil, nil)
    end
  end

  describe "remove_job/3" do
    test "timeout on remove_job" do
      pid = spawn_link(fn -> :timer.sleep(5000) end)

      assert :timeout == remove_job(pid, nil, nil)
    end
  end

  describe "remove_retry/2" do
    test "timeout on remove_retry" do
      pid = spawn_link(fn -> :timer.sleep(5000) end)

      assert :timeout == remove_retry(pid, nil)
    end
  end

  describe "remove_backup/3" do
    test "timeout on remove_backup" do
      pid = spawn_link(fn -> :timer.sleep(5000) end)

      assert :timeout == remove_backup(pid, nil, nil)
    end
  end
end
