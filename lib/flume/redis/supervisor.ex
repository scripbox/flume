defmodule Flume.Redis.Supervisor do
  @doc false
  use Application

  alias Flume.Config

  @redix_worker_prefix "flume_redix"
  @redix_transaction_worker_name :flume_redix_transaction

  def start(_type, _args) do
    if Config.start_on_application() do
      start_link()
    else
      # Don't start Flume
      Supervisor.start_link([], strategy: :one_for_one)
    end
  end

  def start_link() do
    opts = [
      strategy: :one_for_one,
      max_restarts: 20,
      max_seconds: 5,
      name: __MODULE__
    ]

    children = redix_worker_spec() ++ [transaction_pool_spec()]

    {:ok, pid} = Supervisor.start_link(children, opts)

    # Load redis lua scripts
    Flume.Redis.Script.load()

    {:ok, pid}
  end

  def redix_worker_prefix do
    @redix_worker_prefix
  end

  def redix_transaction_worker_name do
    @redix_transaction_worker_name
  end

  # Private API

  defp transaction_pool_spec() do
    opts = [
      {:name, {:local, redix_transaction_worker_name()}},
      {:worker_module, Redix},
      {:size, Config.redis_transaction_pool_size()},
      {:max_overflow, Config.redis_transaction_pool_max_overflow()}
    ]

    :poolboy.child_spec(redix_transaction_worker_name(), opts, Config.connection_opts())
  end

  defp redix_worker_spec() do
    pool_size = Config.redis_pool_size()

    # Create the redix children list of workers:
    for i <- 0..(pool_size - 1) do
      connection_opts =
        Keyword.put(Config.connection_opts(), :name, :"#{redix_worker_prefix()}_#{i}")

      args = Keyword.merge(Config.redis_opts(), connection_opts)
      Supervisor.child_spec({Redix, args}, id: {Redix, i})
    end
  end
end
