defmodule EchoConsumerWithTimestamp do
  use GenStage

  alias Flume.Support.Time, as: TimeExtension

  def start_link(state \\ %{}) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:consumer, state,
     subscribe_to: [{state.upstream, min_demand: 0, max_demand: state.max_demand}]}
  end

  def handle_events(events, _, state) do
    Process.sleep(state.sleep_time)
    send(state.owner, {:received, events, TimeExtension.unix_seconds()})
    {:noreply, [], state}
  end
end
