defmodule Flume.Support.Time do
  import DateTime, only: [utc_now: 0, to_unix: 2, from_unix!: 2]

  def offset_from_now(offset_in_ms, current_time \\ utc_now()) do
    now_in_µs = current_time |> to_unix(:microsecond)
    offset_in_µs = offset_in_ms * 1_000

    now_in_µs
    |> Kernel.+(offset_in_µs)
    |> round()
    |> from_unix!(:microsecond)
  end

  def offset_before(offset_in_ms, current_time \\ utc_now()) do
    now_in_µs = current_time |> to_unix(:microsecond)
    offset_in_µs = offset_in_ms * 1_000

    now_in_µs
    |> Kernel.-(offset_in_µs)
    |> round()
    |> from_unix!(:microsecond)
  end

  def time_to_score(time \\ utc_now()) do
    time
    |> unix_seconds
    |> Float.to_string()
  end

  def unix_seconds(time \\ utc_now()) do
    to_unix(time, :microsecond) / 1_000_000.0
  end

  def format_current_date(current_date) do
    date_time =
      %{current_date | microsecond: {0, 0}}
      |> DateTime.to_string()

    date =
      current_date
      |> DateTime.to_date()
      |> Date.to_string()

    {date_time, date}
  end
end
