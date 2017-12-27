defmodule Flume do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  alias Flume.Config

  def start(_type, _args) do
    import Supervisor.Spec
    # List all child processes to be supervised
    children = [
      # Starts a worker by calling: Flume.Worker.start_link(arg)
      # {Flume.Worker, arg},
      worker(Redix, [redis_opts(), connection_opts()])
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Flume.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def redis_opts do
    host = Config.get(:host)
    port = Config.port
    database = Config.database
    password = Config.get(:password)
    [host: host, port: port, database: database, password: password]
  end

  def connection_opts do
    reconnect_on_sleep = Config.get(:reconnect_on_sleep)
    timeout = Config.get(:redis_timeout)

    [backoff: reconnect_on_sleep, timeout: timeout, name: Flume.Redis]
  end
end
