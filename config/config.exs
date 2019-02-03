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
  reconnect_on_sleep: 100,
  server_shutdown_timeout: 10_000,
  instrumentation: [
    handler_module: Flume.Instrumentation.DefaultEventHandler,
    handler_function: :handle,
    metadata: [app_name: :flume]
  ],
  pipelines: [
    %{
      name: "pipeline_1",
      queue: "default",
      rate_limit_count: 5,
      rate_limit_scale: 1000
    },
    %{
      name: "pipeline_2",
      queue: "low",
      rate_limit_count: 2,
      rate_limit_scale: 5000
    },
    %{
      name: "pipeline_3",
      queue: "priority",
      rate_limit_count: 10,
      rate_limit_scale: 500
    }
  ],
  backoff_initial: 500,
  backoff_max: 10_000,
  scheduler_poll_timeout: 10_000,
  max_retries: 10,
  start_on_application: true

import_config "#{Mix.env()}.exs"
