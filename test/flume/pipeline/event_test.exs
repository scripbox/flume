defmodule Flume.Pipeline.EventTest do
  use Flume.TestWithRedis

  alias Flume.Pipeline.Event
  alias Flume.Redis

  @namespace Flume.Config.namespace()

  describe "pause/2" do
    test "does a permanent pipeline pause" do
      pipeline_name = :default_pipeline
      process_name = Event.Producer.process_name(pipeline_name)
      assert :ok == Event.pause(pipeline_name, async: false, temporary: false, timeout: 5000)

      assert match?(
               %{
                 state: %{
                   paused: true
                 }
               },
               :sys.get_state(process_name)
             )

      assert Redis.Client.get!("#{@namespace}:pipeline:#{pipeline_name}:paused") == "true"
    end

    test "does not set a redis key for temporary pause" do
      pipeline_name = :default_pipeline
      process_name = Event.Producer.process_name(pipeline_name)
      assert :ok == Event.pause(pipeline_name, async: false, temporary: true, timeout: 6000)

      assert match?(
               %{
                 state: %{
                   paused: true
                 }
               },
               :sys.get_state(process_name)
             )

      assert Redis.Client.get!("#{@namespace}:pipeline:#{pipeline_name}:paused") == nil
    end
  end

  describe "resume/2" do
    test "does a permanent pipeline resume" do
      pipeline_name = :default_pipeline
      process_name = Event.Producer.process_name(pipeline_name)
      pause_key = "#{@namespace}:pipeline:#{pipeline_name}:paused"

      # Permanent pause
      assert :ok == Event.pause(pipeline_name, async: false, temporary: false, timeout: 5000)

      assert match?(
               %{
                 state: %{
                   paused: true
                 }
               },
               :sys.get_state(process_name)
             )

      assert Redis.Client.get!(pause_key) == "true"

      # Permanent resume
      assert :ok == Event.resume(pipeline_name, async: false, temporary: false, timeout: 5000)

      assert match?(
               %{
                 state: %{
                   paused: false
                 }
               },
               :sys.get_state(process_name)
             )

      assert Redis.Client.get!(pause_key) == nil
    end

    test "does not delete a redis key for a temporary resume" do
      pipeline_name = :default_pipeline
      process_name = Event.Producer.process_name(pipeline_name)
      pause_key = "#{@namespace}:pipeline:#{pipeline_name}:paused"

      # Permanent pause
      assert :ok == Event.pause(pipeline_name, async: false, temporary: false, timeout: 6000)

      assert match?(
               %{
                 state: %{
                   paused: true
                 }
               },
               :sys.get_state(process_name)
             )

      assert Redis.Client.get!(pause_key) == "true"

      # Temporary resume
      assert :ok == Event.resume(pipeline_name, async: false, temporary: true, timeout: 6000)

      assert match?(
               %{
                 state: %{
                   paused: false
                 }
               },
               :sys.get_state(process_name)
             )

      assert Redis.Client.get!(pause_key) == "true"
    end
  end
end
