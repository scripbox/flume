use Mix.Config

config :flume,
  name: Flume,
  host: {:system, "FLUME_REDIS_HOST", "127.0.0.1"},
  port: {:system, "FLUME_REDIS_PORT", "6379"},
  namespace: "flume_test",
  database: 0,
  redis_timeout: 5000,
  redis_pool_size: 1,
  pipelines: [],
  backoff_initial: 1,
  backoff_max: 2,
  scheduler_poll_interval: 10_000,
  max_retries: 5,
  start_on_application: true

config :logger,
  format: "[$level] $message\n",
  backends: [{LoggerFileBackend, :error_log}],
  level: :warn

config :logger, :error_log,
  path: "log/test.log",
  level: :warn
