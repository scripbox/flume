defmodule TestProducer do
  use GenStage

  # Client API
  def start_link(%{process_name: process_name, queue: _queue} = state) do
    GenStage.start_link(__MODULE__, state, name: String.to_atom(process_name))
  end

  # Server callbacks
  def init(state) do
    {:producer, state}
  end

  def handle_demand(demand, state) when demand > 0 do
    {_count, events} = take(demand, state.queue)

    {:noreply, events, state}
  end

  def handle_call({:consumer_done, _val}, _from, state) do
    {:reply, :ok, [], state}
  end

  # Private API
  defp take(demand, queue_name) do
    events =
      case Flume.fetch_jobs(queue_name, demand) do
        [{:error, _error}] ->
          []

        events ->
          events
      end

    count = length(events)

    {count, events}
  end
end
