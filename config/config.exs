# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

config :flume,
  name: Flume,
  host: {:system, "FLUME_REDIS_HOST", "127.0.0.1"},
  port: {:system, "FLUME_REDIS_PORT", 6379},
  namespace: "flume",
  database: 0,
  redis_timeout: 5000,
  redis_pool_size: 10,
  server_shutdown_timeout: 10_000,
  instrumentation: [
    handler_module: Flume.Instrumentation.DefaultEventHandler,
    handler_function: :handle,
    metadata: [app_name: :flume]
  ],
  pipelines: [],
  backoff_initial: 500,
  backoff_max: 10_000,
  scheduler_poll_interval: 10_000,
  max_retries: 10,
  start_on_application: true

import_config "#{Mix.env()}.exs"
