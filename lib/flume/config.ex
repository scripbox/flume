defmodule Flume.Config do
  use GenServer

  @defaults %{
    backoff_initial: 500,
    backoff_max: 10_000,
    database: 0,
    host: "127.0.0.1",
    mock: false,
    max_retries: 5,
    name: Flume,
    namespace: "flume",
    password: nil,
    pipelines: [],
    port: 6379,
    redis_pool_size: 10,
    redis_timeout: 5000,
    scheduler_poll_interval: 10_000,
    dequeue_lock_ttl: 30_000,
    dequeue_process_timeout: 10_000,
    dequeue_lock_poll_interval: 500,
    debug_log: false,
    # In seconds
    visibility_timeout: 600,
    instrumentation: [
      handler_module: Flume.Instrumentation.DefaultEventHandler,
      handler_function: :handle,
      config: [app_name: :flume]
    ],
    # Redis Sentinels configuration
    sentinels: nil,
    sentinel_group: nil,
    sentinel_socket_opts: [],
    sentinel_reconnect_timeout: 1000
  }

  @integer_keys [
    :port,
    :database,
    :redis_timeout,
    :scheduler_poll_interval,
    :backoff_initial,
    :backoff_max,
    :sentinel_reconnect_timeout
  ]

  alias Flume.Utils.IntegerExtension

  # GenServer callbacks
  def start_link(runtime_config \\ %{}) do
    GenServer.start_link(__MODULE__, runtime_config, name: __MODULE__)
  end

  def init(runtime_config) do
    {:ok, runtime_config}
  end

  def handle_call({:get, key}, _from, runtime_config) do
    value = get_runtime_or_app_config(key, runtime_config)
    {:reply, value, runtime_config}
  end

  def handle_call({:get, key, fallback}, _from, runtime_config) do
    value = get_runtime_or_app_config(key, runtime_config, fallback)
    {:reply, value, runtime_config}
  end

  def handle_call(:get_runtime_config, _from, runtime_config) do
    {:reply, runtime_config, runtime_config}
  end

  def handle_cast({:put, key, value}, runtime_config) do
    {:noreply, Map.put(runtime_config, key, value)}
  end

  # Public API
  Map.keys(@defaults)
  |> Enum.each(fn key ->
    def unquote(key)(), do: get(unquote(key))
  end)

  def get(key), do: get(key, default(key))

  def get(key, fallback) do
    case Process.whereis(__MODULE__) do
      nil ->
        # Fallback to application config if GenServer not started
        get_app_config(key, fallback)

      _pid ->
        GenServer.call(__MODULE__, {:get, key, fallback})
    end
  end

  def put(key, value) do
    GenServer.cast(__MODULE__, {:put, key, value})
  end

  def get_runtime_config do
    case Process.whereis(__MODULE__) do
      nil -> %{}
      _pid -> GenServer.call(__MODULE__, :get_runtime_config)
    end
  end

  # Private functions
  defp get_runtime_or_app_config(key, runtime_config, fallback \\ nil) do
    case Map.get(runtime_config, key) do
      nil -> get_app_config(key, fallback || default(key))
      value -> parse(key, value)
    end
  end

  defp get_app_config(key, fallback) do
    value =
      case Application.get_env(:flume, key, fallback) do
        {:system, varname} -> System.get_env(varname)
        {:system, varname, default} -> System.get_env(varname) || default
        value -> value
      end

    parse(key, value)
  end

  defp default(key), do: Map.get(@defaults, key)

  defp parse(key, value) when key in @integer_keys do
    case IntegerExtension.parse(value) do
      :error ->
        raise Flume.Errors.InvalidConfiguration, key

      parsed_value ->
        parsed_value
    end
  end

  defp parse(_key, {:system, env_var, default_value}) do
    System.get_env(env_var, default_value)
  end

  defp parse(_key, value), do: value

  def redis_opts(opts \\ []) do
    base_opts = [
      host: Keyword.get(opts, :host, host()),
      port: Keyword.get(opts, :port, port()),
      database: Keyword.get(opts, :database, database()),
      password: Keyword.get(opts, :password, password())
    ]

    # Add sentinel configuration if available
    case {sentinels(), sentinel_group()} do
      {nil, _} ->
        base_opts

      {sentinels, nil} when is_list(sentinels) ->
        base_opts

      {sentinels, group} when is_list(sentinels) and is_binary(group) ->
        # Remove host and port when using sentinels as Redix doesn't allow both
        sentinel_opts = [
          sentinel: [
            sentinels: sentinels,
            group: group,
            socket_opts: sentinel_socket_opts(),
            timeout: sentinel_reconnect_timeout()
          ]
        ]

        base_opts
        |> Keyword.delete(:host)
        |> Keyword.delete(:port)
        |> Keyword.merge(sentinel_opts)

      _ ->
        base_opts
    end
  end

  def connection_opts(opts \\ []) do
    [timeout: Keyword.get(opts, :timeout, redis_timeout())]
  end

  def scheduler_opts do
    [
      namespace: namespace(),
      scheduler_poll_interval: scheduler_poll_interval()
    ]
  end

  def queues, do: Enum.map(pipelines(), & &1.queue)

  def pipeline_names, do: Enum.map(pipelines(), & &1.name)

  def queue_api_module do
    case mock() do
      false ->
        Flume.Queue.DefaultAPI

      true ->
        Flume.Queue.MockAPI
    end
  end

  def pipeline_api_module do
    case mock() do
      false ->
        Flume.Pipeline.DefaultAPI

      true ->
        Flume.Pipeline.MockAPI
    end
  end

  @doc """
  Validates Redis Sentinels configuration

  ## Examples

  Basic sentinel configuration:

      sentinels: [
        [host: "sentinel1.example.com", port: 26379],
        [host: "sentinel2.example.com", port: 26379]
      ],
      sentinel_group: "mymaster"

  Or using Redis URIs:

      sentinels: [
        "redis://sentinel1.example.com:26379",
        "redis://sentinel2.example.com:26379"
      ],
      sentinel_group: "mymaster"
  """
  def valid_sentinel_config? do
    case {sentinels(), sentinel_group()} do
      {nil, _} -> true
      {sentinels, group} when is_list(sentinels) and is_binary(group) ->
        Enum.all?(sentinels, &valid_sentinel_entry?/1)
      _ -> false
    end
  end

  defp valid_sentinel_entry?(uri) when is_binary(uri) do
    # Validate Redis URI format
    uri =~ ~r/^redis:\/\//
  end

  defp valid_sentinel_entry?(%{host: host, port: port})
    when is_binary(host) and is_integer(port), do: true
  defp valid_sentinel_entry?([host: host, port: port])
    when is_binary(host) and is_integer(port), do: true
  defp valid_sentinel_entry?(_), do: false
end
