defmodule Flume.JobFactory do
  def generate_jobs(module_name, count) do
    Enum.map(1..count, fn _ -> generate(module_name) end)
  end

  def generate(module_name, args \\ []) do
    %{
      class: module_name,
      function: "perform",
      queue: "test",
      jid: "1082fd87-2508-4eb4-8fba-#{:rand.uniform(9_999_999)}a60e3",
      args: args,
      retry_count: 0,
      enqueued_at: 1_514_367_662,
      finished_at: nil,
      failed_at: nil,
      retried_at: nil,
      error_message: nil,
      error_backtrace: nil
    }
    |> Jason.encode!()
  end
end
