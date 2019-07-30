# Flume

[![pipeline status](https://code.scripbox.io/packages/flume/badges/master/pipeline.svg?style=flat-square)](https://code.scripbox.io/packages/flume/commits/master)
[![coverage report](https://code.scripbox.io/packages/flume/badges/master/coverage.svg?style=flat-square)](https://code.scripbox.io/packages/flume/commits/master)

Flume is a job processing system backed by [Redis](https://redis.io/) & [GenStage](https://github.com/elixir-lang/gen_stage).

## Table of Contents

- [Features](#Features)
- [Requirements](#Requirements)
- [Installation](#Installation)
- [Usage](#Usage)
  - [Configuring Pipelines](#Configuring-Pipelines)
  - [Rate Limiting](#Rate-Limiting)
  - [Batch Processing](#Batch-Processing)
  - [Creating Workers](#Creating-Workers)
  - [Enqueuing Jobs](#Enqueueing-Jobs)
  - [Instrumentation](#Instrumentation)
- [Testing](#Testing)
- [Troubleshooting](#Troubleshooting)
- [Roadmap](#Roadmap)
- [Contributing](#Contributing)

## Features

- Store all jobs in Redis
- Configure the queues in Redis
- Define a supervised producer for each queue in Redis
- Configure the concurrency (no. of consumers) for each queue
- A producer pulls jobs from Redis via a separate supervised connection module (Redis.Connection)
- Each connection process holds its own connection to Redis
- Each connection process will pop jobs from a specific queue from Redis
- Configure the min & max demand for each kind of consumer
- Add rate-limit for each queue in Redis
- Handle error/exception in Consumer while processing a job
- Enqueue the job to the retry queue/pipeline
- Jobs should be retried with exponential backoff
- Should have a separate logger

## Requirements

- Elixir 1.6.6+
- Erlang/OTP 21.1+
- Redis 4.0+

## Installation

Add Flume to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:flume, "~> 0.1.0"}
  ]
end
```

Then run `mix deps.get` to install Flume and its dependencies.

## Usage

Add Flume supervisor to your application's supervision tree:

```elixir
defmodule MyApplication.Application do
  use Application

  import Supervisor.Spec

  def start(_type, _args) do
    children = [
      # Start Flume supervisor
      supervisor(Flume, [])
    ]

    opts = [strategy: :one_for_one, name: MyApplication.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

Add `config/flume.exs`:

```elixir
config :flume,
  name: Flume,
  host: "127.0.0.1",
  port: "6379",
  namespace: "my-app",
  database: 0,
  redis_pool_size: 10,
  redis_timeout: 10_000,
  backoff_initial: 30_000,
  backoff_max: 36_00_000,
  scheduler_poll_interval: 10_000,
  max_retries: 15,
  visibility_timeout: 600,
  dequeue_lock_ttl: 30_000,
  dequeue_process_timeout: 10_000,
  dequeue_lock_poll_interval: 500
```

#### Configuring Pipelines

```elixir
config :flume,
  pipelines: [
    %{name: "default_pipeline", queue: "default"},
    %{name: "low_pipeline", queue: "low"}
  ]
```

#### Rate Limiting

[TODO]

#### Batch Processing

[TODO]

#### Creating Workers

[TODO]

#### Enqueuing Jobs

[TODO]

#### Instrumentation

[TODO]

## Testing

[TODO]

## Troubleshooting

[TODO]

## Roadmap

* Support multiple queue backends (right now only Redis is supported)

## Contributing

[TODO]
