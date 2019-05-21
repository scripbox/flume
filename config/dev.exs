use Mix.Config

config :flume,
  name: Flume,
  host: {:system, "FLUME_REDIS_HOST", "127.0.0.1"},
  port: {:system, "FLUME_REDIS_PORT", "6379"},
  namespace: "flume_dev",
  database: 0,
  redis_timeout: 5000,
  redis_pool_size: 5,
  server_shutdown_timeout: 10_000,
  pipelines: [
    %{
      name: "default-pipeline",
      queue: "default",
      max_demand: 5
    },
    %{
      name: "low-pipeline",
      queue: "low",
      max_demand: 2
    },
    %{
      name: "priority-pipeline",
      queue: "priority",
      max_demand: 10
    },
    %{
      name: "limited-pipeline",
      queue: "limited",
      max_demand: 5,
      rate_limit_count: 10,
      rate_limit_scale: 10_000,
      rate_limit_key: "limited",
      instrument: true
    },
    %{
      name: "batch-pipeline",
      queue: "batch",
      max_demand: 5,
      batch_size: 2,
      instrument: true
    }
  ],
  backoff_initial: 500,
  backoff_max: 10_000,
  scheduler_poll_interval: 10_000,
  max_retries: 10,
  start_on_application: true

config :logger, backends: [{LoggerFileBackend, :error_log}]

config :logger, :error_log,
  format: "$date $time $metadata [$level] $message\n",
  path: "log/dev.log",
  level: :debug
