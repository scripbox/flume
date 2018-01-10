use Mix.Config

config :flume,
  name: Flume,
  host: {:system, "FLUME_REDIS_HOST", "127.0.0.1"},
  port: {:system, "FLUME_REDIS_PORT", "6379"},
  namespace: "flume_dev",
  database: 0,
  redis_timeout: 5000,
  reconnect_on_sleep: 100,
  poll_timeout: 500,
  pipelines: [
    %{name: "pipeline_1", queue: "default", concurrency: 4},
    %{name: "pipeline_2", queue: "low", concurrency: 2, rate_limit_count: 2, rate_limit_scale: 5000},
    %{name: "pipeline_3", queue: "priority", concurrency: 6, rate_limit_count: 10, rate_limit_scale: 1000}
  ],
  backoff_initial: 500,
  backoff_max: 10_000,
  scheduler_poll_timeout: 10_000,
  max_retries: 10

config :logger, backends: [{LoggerFileBackend, :error_log}]

config :logger, :error_log,
  format: "$date $time $metadata [$level] $message\n",
  path: "log/dev.log",
  level: :debug
