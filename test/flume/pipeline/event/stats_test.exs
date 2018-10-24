defmodule Flume.Pipeline.Event.StatsTest do
  use TestWithRedis
  use TestWithEts

  alias Flume.Pipeline.Event.Stats
  @redis_namespace Flume.Config.get(:namespace)

  describe "persist/0" do
    test "persist pipeline stats to Redis" do
      pipeline_name = "test_pipeline"

      Stats.register(pipeline_name)

      command =
        ~w(MGET #{@redis_namespace}:stat:processed:#{pipeline_name} #{@redis_namespace}:stat:failed:#{
          pipeline_name
        })

      assert {:ok, [nil, nil]} == Flume.Redis.Client.query(command)

      Stats.incr(:processed, pipeline_name, 1)
      Stats.incr(:processed, pipeline_name, 1)
      Stats.incr(:failed, pipeline_name, 1)

      Stats.persist()
      assert {:ok, ["2", "1"]} == Flume.Redis.Client.query(command)
    end
  end
end
