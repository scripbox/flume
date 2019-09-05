defmodule Flume.Redis.Supervisor do
  @doc false
  use Application

  alias Flume.Config

  @redix_worker_prefix "flume_redix"

  def start(_type, _args) do
    if Config.start_on_application() do
      start_link()
    else
      # Don't start Flume
      Supervisor.start_link([], strategy: :one_for_one)
    end
  end

  def start_link(opts \\ []) do
    sup_opts = [
      strategy: :one_for_one,
      max_restarts: 20,
      max_seconds: 5,
      name: __MODULE__
    ]

    {:ok, pid} = Supervisor.start_link(redix_worker_spec(opts), sup_opts)

    # Load redis lua scripts
    Flume.Redis.Script.load()
    |> case do
      :ok ->
        {:ok, pid}

      error ->
        {:shutdown, error}
    end
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      shutdown: 500
    }
  end

  def redix_worker_prefix do
    @redix_worker_prefix
  end

  # Private API

  defp redix_worker_spec(options) do
    pool_size = Config.redis_pool_size()

    # Create the redix children list of workers:
    for i <- 0..(pool_size - 1) do
      connection_opts =
        Keyword.put(Config.connection_opts(options), :name, :"#{redix_worker_prefix()}_#{i}")

      args = Keyword.merge(Config.redis_opts(options), connection_opts)
      Supervisor.child_spec({Redix, args}, id: {Redix, i})
    end
  end
end
