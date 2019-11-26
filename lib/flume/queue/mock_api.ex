defmodule Flume.Queue.MockAPI do
  @behaviour Flume.Queue.API

  def bulk_enqueue(queue, jobs, opts \\ [])

  def bulk_enqueue(queue, jobs, []) do
    message = %{queue: queue, jobs: jobs}

    send(self(), message)

    {:ok, message}
  end

  def bulk_enqueue(queue, jobs, opts) do
    send(self(), %{queue: queue, jobs: jobs, options: opts})
  end

  def enqueue(
        queue,
        worker,
        function_name \\ :perform,
        args,
        opts \\ []
      )

  def enqueue(
        queue,
        worker,
        function_name,
        args,
        []
      ) do
    message = %{queue: queue, worker: worker, function_name: function_name, args: args}

    send(self(), message)

    {:ok, message}
  end

  def enqueue(
        queue,
        worker,
        function_name,
        args,
        opts
      ) do
    message = %{queue: queue, worker: worker, function_name: function_name, args: args, options: opts}

    send(self(), message)

    {:ok, message}
  end

  def enqueue_in(
        queue,
        time_in_seconds,
        worker,
        function_name \\ :perform,
        args,
        opts \\ []
      )

  def enqueue_in(
        queue,
        time_in_seconds,
        worker,
        function_name,
        args,
        []
      ) do
    message = %{
      schedule_in: time_in_seconds,
      queue: queue,
      worker: worker,
      function_name: function_name,
      args: args
    }

    send(self(), message)

    {:ok, message}
  end

  def enqueue_in(
        queue,
        time_in_seconds,
        worker,
        function_name,
        args,
        opts
      ) do
    message = %{
        schedule_in: time_in_seconds,
        queue: queue,
        worker: worker,
        function_name: function_name,
        args: args,
        options: opts
      }

    send(self(), message)

    {:ok, message}
  end
end
