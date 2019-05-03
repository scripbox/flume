defmodule Flume.Pipeline.Event.WorkerTest do
  use TestWithRedis

  alias Flume.{Event, BulkEvent}
  alias Flume.Pipeline.Event.Worker

  describe "process/2" do
    test "processes single event" do
      pipeline = %{
        name: "Pipeline1",
        queue: "default",
        rate_limit_count: 1000,
        rate_limit_scale: 5000
      }

      caller_name = :calling_process
      message = "hello world"

      Process.register(self(), caller_name)

      serialized_event =
        %{event_attributes() | "args" => [caller_name, message]} |> Jason.encode!()

      # Start the worker process
      {:ok, _pid} = Worker.start_link(pipeline, serialized_event)

      assert_receive {:received, ^message}
    end

    test "emits telemetry event on successful pipelines" do
      pipeline = %{
        name: "Pipeline1",
        queue: "default",
        rate_limit_count: 1000,
        rate_limit_scale: 5000,
        instrument: true
      }

      pipeline_name_atom = String.to_atom(pipeline.name)

      {test_name, _arity} = __ENV__.function

      parent = self()

      handler = fn event, measurements, meta, _config ->
        send(parent, {event, measurements, meta})
      end

      :telemetry.attach(
        [to_string(test_name), :worker, :duration],
        [pipeline_name_atom, :worker, :duration],
        handler,
        :no_config
      )

      :telemetry.attach(
        [to_string(test_name), :worker, :job, :duration],
        [pipeline_name_atom, :worker, :job, :duration],
        handler,
        :no_config
      )

      caller_name = :calling_process
      message = "hello world"

      Process.register(self(), caller_name)

      serialized_event =
        %{event_attributes() | "args" => [caller_name, message]} |> Jason.encode!()

      # Start the worker process
      {:ok, _pid} = Worker.start_link(pipeline, serialized_event)

      assert_receive {
        [^pipeline_name_atom, :worker, :job, :duration],
        %{value: value},
        %{module: "echoworker"}
      }

      assert value >= 0

      assert_receive {
        [^pipeline_name_atom, :worker, :duration],
        %{value: value},
        %{module: "echoworker"}
      }

      assert value >= 0

      :telemetry.detach(to_string(test_name))
    end

    test "processes bulk event" do
      pipeline = %{
        name: "Pipeline1",
        queue: "default",
        rate_limit_count: 1000,
        rate_limit_scale: 5000,
        batch_size: 10
      }

      caller_name = :calling_process
      message = "hello world"

      Process.register(self(), caller_name)

      single_event = %{event_attributes() | "args" => [caller_name, message]} |> Event.new()
      bulk_event = BulkEvent.new(single_event)

      # Start the worker process
      {:ok, _pid} = Worker.start_link(pipeline, bulk_event)

      assert_receive {:received, ^message}
    end
  end

  def event_attributes do
    %{
      "class" => "EchoWorker",
      "function" => "perform",
      "queue" => "test",
      "jid" => "1082fd87-2508-4eb4-8fba-2958584a60e3",
      "args" => [],
      "retry_count" => 0,
      "enqueued_at" => 1_514_367_662,
      "finished_at" => nil,
      "failed_at" => nil,
      "retried_at" => nil,
      "error_message" => nil,
      "error_backtrace" => nil
    }
  end
end
