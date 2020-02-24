defmodule Flume.PipelineTest do
  use ExUnit.Case, async: true

  import Flume.Mock

  alias Flume.Pipeline

  describe "pause/2" do
    test "returns error for an invalid pipeline" do
      assert Pipeline.pause("invalid-pipeline", []) ==
               {:error, "pipeline invalid-pipeline has not been configured"}

      assert Pipeline.pause(123, []) == {:error, "invalid value for a pipeline name"}
    end

    test "returns error for invalid options" do
      assert Pipeline.pause("default_pipeline", async: 0) ==
               {:error, "expected :async to be a boolean, got: 0"}
    end

    test "crashes with an exit signal when the pipeline pause timeouts" do
      try do
        Pipeline.pause("default_pipeline", async: false, temporary: true, timeout: 0)
      catch
        :exit, error ->
          assert error == {:timeout, {GenServer, :call, [:default_pipeline_producer, :pause, 0]}}
      end
    end

    test "pauses a valid pipeline" do
      with_flume_mock do
        Pipeline.pause("default_pipeline", async: true, temporary: true, timeout: 300)

        assert_receive(%{
          action: :pause,
          pipeline_name: "default_pipeline",
          options: [temporary: true, timeout: 300, async: true]
        })
      end
    end
  end

  describe "resume/2" do
    test "returns error for an invalid pipeline" do
      assert Pipeline.resume("invalid-pipeline", []) ==
               {:error, "pipeline invalid-pipeline has not been configured"}

      assert Pipeline.resume(123, []) == {:error, "invalid value for a pipeline name"}
    end

    test "returns error for invalid options" do
      assert Pipeline.resume("default_pipeline", temporary: 0) ==
               {:error, "expected :temporary to be a boolean, got: 0"}
    end

    test "resumes a valid pipeline" do
      name = :default_pipeline
      process_name = Pipeline.Event.Producer.process_name(name)
      assert :ok == Pipeline.pause(name, async: false, temporary: true)

      assert match?(
               %{
                 state: %{
                   paused: true
                 }
               },
               :sys.get_state(process_name)
             )

      assert :ok == Pipeline.resume(name, async: false, temporary: true, timeout: 4000)

      assert match?(
               %{
                 state: %{
                   paused: false
                 }
               },
               :sys.get_state(process_name)
             )
    end

    test "crashes with an exit signal when the pipeline resume timeouts" do
      try do
        Pipeline.resume("default_pipeline", async: false, temporary: true, timeout: 0)
      catch
        :exit, error ->
          assert error == {:timeout, {GenServer, :call, [:default_pipeline_producer, :resume, 0]}}
      end
    end
  end
end
