defmodule Flume.Pipeline.Event.Producer do
  @moduledoc """
  Polls for a batch of events from the source (Redis queue).
  This stage acts as a Producer in the GenStage pipeline.

  [**Producer**] <- ProducerConsumer <- Consumer
  """
  use GenStage

  require Flume.Logger

  alias Flume.Logger
  alias Flume.Pipeline.Event, as: EventPipeline

  # 2 seconds
  @default_interval 2000

  # Client API
  def start_link(%{name: pipeline_name, queue: _queue} = state) do
    GenStage.start_link(__MODULE__, state, name: process_name(pipeline_name))
  end

  def pause(pipeline_name) do
    GenStage.call(process_name(pipeline_name), :pause)
  end

  def resume(pipeline_name) do
    GenStage.call(process_name(pipeline_name), :resume)
  end

  # Server callbacks
  def init(%{name: name} = state) do
    state =
      state
      |> Map.put(:paused, EventPipeline.paused_state(name))
      |> Map.put(:demand, 0)

    {:producer, state}
  end

  def handle_demand(demand, state) when demand > 0 do
    Logger.debug("#{state.name} [Producer] handling demand of #{demand}")

    new_demand = state.demand + demand
    state = Map.put(state, :demand, new_demand)

    dispatch_events(state)
  end

  def handle_info(:fetch_events, state) do
    # This callback is invoked by the Process.send_after/3 message below.
    dispatch_events(state)
  end

  def handle_call(:pause, _from, %{paused: true} = state) do
    {:reply, :ok, [], state}
  end

  def handle_call(:pause, _from, state) do
    state = Map.put(state, :paused, true)

    {:reply, :ok, [], state}
  end

  def handle_call(:resume, _from, %{paused: true} = state) do
    state = Map.put(state, :paused, false)

    {:reply, :ok, [], state}
  end

  def handle_call(:resume, _from, state) do
    {:reply, :ok, [], state}
  end

  defp dispatch_events(%{paused: true} = state) do
    schedule_fetch_events(state)

    {:noreply, [], state}
  end

  defp dispatch_events(%{demand: demand, batch_size: nil} = state) when demand > 0 do
    Logger.debug("#{state.name} [Producer] pulling #{demand} events")

    {count, events} = take(demand, state)

    Logger.debug("#{state.name} [Producer] pulled #{count} events from source")

    new_demand = state.demand - count
    state = Map.put(state, :demand, new_demand)

    schedule_fetch_events(state)

    {:noreply, events, state}
  end

  defp dispatch_events(%{demand: demand, batch_size: batch_size} = state)
       when demand > 0 and batch_size > 0 do
    events_to_ask = demand * batch_size

    Logger.debug("#{state.name} [Producer] pulling #{events_to_ask} events")

    {count, events} = take(events_to_ask, state)

    Logger.debug("#{state.name} [Producer] pulled #{count} events from source")

    new_demand = demand - round(count / batch_size)
    state = Map.put(state, :demand, new_demand)

    schedule_fetch_events(state)

    {:noreply, events, state}
  end

  defp dispatch_events(state) do
    schedule_fetch_events(state)

    {:noreply, [], state}
  end

  defp schedule_fetch_events(%{demand: demand} = _state) when demand > 0 do
    # Schedule the next request
    Process.send_after(self(), :fetch_events, @default_interval)
  end

  defp schedule_fetch_events(_state), do: nil

  # For regular pipelines
  defp take(demand, %{rate_limit_count: nil, rate_limit_scale: nil} = state) do
    Flume.fetch_jobs(state.queue, demand)
    |> do_take(state.queue)
  end

  # For rate-limited pipelines
  defp take(
         demand,
         %{rate_limit_count: rate_limit_count, rate_limit_scale: rate_limit_scale} = state
       ) do
    Flume.fetch_jobs(
      state.queue,
      demand,
      %{
        rate_limit_count: rate_limit_count,
        rate_limit_scale: rate_limit_scale,
        rate_limit_key: Map.get(state, :rate_limit_key)
      }
    )
    |> do_take(state.queue)
  end

  defp do_take(events, queue_name) do
    case events do
      {:error, error} ->
        Logger.error("#{queue_name} [Producer] error: #{error.reason}")
        {0, []}

      {:ok, events} ->
        {length(events), events}
    end
  end

  def process_name(pipeline_name), do: :"#{pipeline_name}_producer"
end
