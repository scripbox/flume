import Config

# Configure Flume for testing
config :flume,
  name: Flume,
  host: {:system, "FLUME_REDIS_HOST", "127.0.0.1"},
  port: {:system, "FLUME_REDIS_PORT", "6379"},
  namespace: "flume_test",
  database: 1,
  redis_timeout: 5000,
  redis_pool_size: 1,
  pipelines: [
    %{
      name: "default_pipeline",
      queue: "default",
      max_demand: 1000
    },
    %{
      name: "test_pipeline",
      queue: "test",
      max_demand: 1000
    }
  ],
  backoff_initial: 1,
  backoff_max: 2,
  scheduler_poll_interval: 1000,
  max_retries: 3

config :logger,
  format: "[$level] $message\n",
  backends: [{LoggerFileBackend, :error_log}],
  level: :warning

config :logger, :error_log,
  path: "log/test.log",
  level: :warning
