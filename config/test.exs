use Mix.Config

config :flume,
  name: Flume,
  host: {:system, "FLUME_REDIS_HOST", "127.0.0.1"},
  port: {:system, "FLUME_REDIS_PORT", "6379"},
  namespace: "flume_test",
  database: 0,
  redis_timeout: 5000,
  reconnect_on_sleep: 100,
  poll_timeout: 500,
  pipelines: [],
  backoff_initial: 500,
  backoff_max: 10_000,
  scheduler_poll_timeout: 10_000

config :logger, format: "[$level] $message\n",
  backends: [{LoggerFileBackend, :error_log}],
  level: :warn

config :logger, :error_log,
  path: "log/test.log",
  level: :warn
