defmodule Flume.Job.WorkerTest do
  use ExUnit.Case

  alias Flume.Job.Worker

  setup _context do
    {:ok, worker_pid} = Worker.start_link([])

    [worker_pid: worker_pid]
  end

  def event_attributes do
    %{
      class: "EchoWorker",
      function: "perform",
      queue: "test",
      jid: "1082fd87-2508-4eb4-8fba-2958584a60e3",
      args: [],
      retry_count: 0,
      enqueued_at: 1_514_367_662,
      finished_at: nil,
      failed_at: nil,
      retried_at: nil,
      error_message: nil,
      error_backtrace: nil
    }
  end

  describe "execute/3" do
    test "successfully executes job without error", %{worker_pid: worker_pid} do
      pipeline_name = "pipeline"
      caller_name = :calling_process
      message = "hello world"

      Flume.PipelineStats.register(pipeline_name)
      Process.register(self(), caller_name)

      assert :ok == Worker.execute(
        worker_pid,
        %{event_attributes() | args: [caller_name, message]} |> Poison.encode!(),
        pipeline_name
      )
    end
  end

  describe "handle_cast/3" do
    test "successfully executes job without error" do
      pipeline_name = "pipeline"
      caller_name = :calling_process
      message = "hello world"

      Flume.PipelineStats.register(pipeline_name)
      Process.register(self(), caller_name)

      assert {:noreply, %{}} == Worker.handle_cast(
        {
          :execute,
          %{event_attributes() | args: [caller_name, message]} |> Poison.encode!(),
          pipeline_name
        },
        %{}
      )
    end
  end
end
