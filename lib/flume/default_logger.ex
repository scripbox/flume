defmodule Flume.DefaultLogger do
  @behaviour Flume.Logger

  require Logger

  def debug(message, %{}), do: Logger.debug(message)
  def debug(message, opts), do: Logger.debug("#{message} - #{inspect(opts)}")

  def error(message, %{}), do: Logger.error(message)
  def error(message, opts), do: Logger.error("#{message} - #{inspect(opts)}")

  def info(message, %{}), do: Logger.info(message)
  def info(message, opts), do: Logger.info("#{message} - #{inspect(opts)}")
end
