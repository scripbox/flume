defmodule Flume.Utils.Retry do
  require Logger

  alias Flume.Utils.Retry

  @default_max_tries 5

  @backoff_exponent 1.5
  @backoff_initial 500
  @backoff_max 10_000

  defmacro retry(count \\ 0, do: yield) do
    quote do
      fn -> unquote(yield) end |> Retry.retry_block(unquote(count))
    end
  end

  def retry_block(func, count, exception \\ nil, max_tries \\ @default_max_tries)

  def retry_block(func, count, _exception, max_tries) when count < max_tries do
    apply(func, [])
  rescue
    e in _ ->
      Logger.error("#{__MODULE__} Failed to execute the code with error - #{inspect(e)}")

      calc_next_backoff(count) |> :timer.sleep()

      Logger.error("#{__MODULE__} Retrying #{count + 1} times")

      retry_block(func, count + 1, e, max_tries)
  end

  def retry_block(_fun, count, exception, max_tries) when count == max_tries do
    Logger.error("#{__MODULE__} Max tries exceeded")

    exception
  end

  defp calc_next_backoff(count) do
    next_exponential_backoff = round(@backoff_initial * count * @backoff_exponent)

    if @backoff_max == :infinity do
      next_exponential_backoff
    else
      min(next_exponential_backoff, @backoff_max)
    end
  end
end
