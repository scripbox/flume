defmodule FLume.ProducerTest do
  use TestWithRedis

  alias Flume.Producer
  alias Flume.Redis.Job

  @namespace Flume.Config.get(:namespace)

  def serialized_job do
    %{
      class: "Elixir.Worker",
      queue: "test",
      jid: "1089fd87-2508-4eb4-8fba-2958584a60e3",
      enqueued_at: 1514367662,
      args: [1]
    } |> Poison.encode!
  end

  describe "handle_demand/2" do
    test "pull events from redis" do
      state = %{name: "pipeline_1", queue: "test"}
      downstream_name = Enum.join([state.name, "producer_consumer"], "_") |> String.to_atom

      Enum.each(1..3, fn(_) ->
        Job.enqueue(Flume.Redis, "#{@namespace}:test", serialized_job())
      end)

      {:ok, producer} = Producer.start_link(state)
      {:ok, _} = EchoConsumer.start_link(producer, self(), name: downstream_name)

      events = serialized_job()
      Enum.each(1..3, fn(_) ->
        assert_receive {:received, [^events]}
      end)

      # The consumer will also stop, since it is subscribed to the stage
      GenStage.stop(producer)
    end
  end
end
