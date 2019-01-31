use Mix.Config

config :flume,
  name: Flume,
  host: {:system, "FLUME_REDIS_HOST", "127.0.0.1"},
  port: {:system, "FLUME_REDIS_PORT", "6379"},
  namespace: "flume_dev",
  database: 0,
  redis_timeout: 5000,
  redis_pool_size: 5,
  reconnect_on_sleep: 100,
  poll_timeout: 500,
  server_shutdown_timeout: 10_000,
  pipelines: [
    %{name: "default_pipeline", queue: "default"},
    %{
      name: "low_pipeline",
      queue: "low",
      rate_limit_count: 2,
      rate_limit_scale: 5000
    },
    %{
      name: "high_pipeline",
      queue: "priority",
      rate_limit_count: 10,
      rate_limit_scale: 1000,
      instrument: true
    },
    %{
      name: "batch_pipeline",
      queue: "batch",
      rate_limit_count: 100,
      rate_limit_scale: 5000,
      batch_size: 5,
      instrument: true
    }
  ],
  backoff_initial: 500,
  backoff_max: 10_000,
  scheduler_poll_timeout: 10_000,
  max_retries: 10,
  start_on_application: true

config :logger, backends: [{LoggerFileBackend, :error_log}]

config :logger, :error_log,
  format: "$date $time $metadata [$level] $message\n",
  path: "log/dev.log",
  level: :debug
