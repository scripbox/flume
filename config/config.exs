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
  reconnect_on_sleep: 100,
  pipelines: [
    %{name: "pipeline_1", queue: "default", concurrency: 4},
    %{name: "pipeline_2", queue: "low", concurrency: 2},
    %{name: "pipeline_3", queue: "priority", concurrency: 6}
  ]

import_config "#{Mix.env}.exs"
