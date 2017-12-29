defmodule Flume.Queue.Backoff do
  @backoff_exponent 1.5

  alias Flume.Config

  def calc_next_backoff(count) do
    backoff_current = Config.backoff_initial * count
    backoff_max = Config.backoff_max
    next_exponential_backoff = round(backoff_current * @backoff_exponent)

    if backoff_max == :infinity do
      next_exponential_backoff
    else
      min(next_exponential_backoff, backoff_max)
    end
  end
end
