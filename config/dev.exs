config :flume,
  name: Flume,
  host: "127.0.0.1",
  port: 6379,
  namespace: "flume",
  database: 0,
  redis_timeout: 5000,
  reconnect_on_sleep: 100,
  pipelines: [
    %{name: "Pipeline1", queue: "default", concurrency: 10},
    %{name: "Pipeline2", queue: "default", concurrency: 10}
  ]
