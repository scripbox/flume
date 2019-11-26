defmodule Flume.Queue.MockAPI do
  @behaviour Flume.Queue.API

  def bulk_enqueue(queue, jobs, opts \\ [])

  def bulk_enqueue(queue, jobs, []) do
    send(self(), %{queue: queue, jobs: jobs})
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
    send(
      self(),
      %{queue: queue, worker: worker, function_name: function_name, args: args}
    )
  end

  def enqueue(
        queue,
        worker,
        function_name,
        args,
        opts
      ) do
    send(
      self(),
      %{queue: queue, worker: worker, function_name: function_name, args: args, options: opts}
    )
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
    send(
      self(),
      %{
        schedule_in: time_in_seconds,
        queue: queue,
        worker: worker,
        function_name: function_name,
        args: args
      }
    )
  end

  def enqueue_in(
        queue,
        time_in_seconds,
        worker,
        function_name,
        args,
        opts
      ) do
    send(
      self(),
      %{
        schedule_in: time_in_seconds,
        queue: queue,
        worker: worker,
        function_name: function_name,
        args: args,
        options: opts
      }
    )
  end
end
