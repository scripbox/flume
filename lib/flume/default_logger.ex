defmodule Flume.DefaultLogger do
  @behaviour Flume.Logger

  require Logger

  def debug(message, %{}) do
    if Flume.Config.debug_log(), do: Logger.debug(message)
  end

  def debug(message, opts) do
    if Flume.Config.debug_log(), do: Logger.debug("#{message} - #{inspect(opts)}")
  end

  def error(message, %{}), do: Logger.error(message)
  def error(message, opts), do: Logger.error("#{message} - #{inspect(opts)}")

  def info(message, %{}), do: Logger.info(message)
  def info(message, opts), do: Logger.info("#{message} - #{inspect(opts)}")

  def warn(message, %{}), do: Logger.warn(message)
  def warn(message, opts), do: Logger.warn("#{message} - #{inspect(opts)}")
end
