defmodule EchoConsumer do
  use GenStage

  def start_link(producer, owner, options) do
    GenStage.start_link(
      __MODULE__,
      %{
        producer: producer,
        owner: owner,
        max_demand: Keyword.get(options, :max_demand, 1),
        min_demand: Keyword.get(options, :min_demand, 0)
      },
      name: Keyword.fetch!(options, :name)
    )
  end

  def init(%{producer: producer, owner: owner, min_demand: min_demand, max_demand: max_demand}) do
    {:consumer, owner, subscribe_to: [{producer, min_demand: min_demand, max_demand: max_demand}]}
  end

  def handle_events(events, _, owner) do
    send(owner, {:received, events})
    {:noreply, [], owner}
  end
end
