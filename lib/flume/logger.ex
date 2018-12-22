defmodule Flume.Logger do
  @moduledoc """
  Behaviour module for logging to ensure that
  the following callbacks are implemented
  """

  @callback debug(String.t(), map()) :: :ok | :error
  @callback error(String.t(), map()) :: :ok | :error
  @callback info(String.t(), map()) :: :ok | :error

  defmacro debug(message) do
    quote location: :keep do
      apply(Flume.Config.logger(), :debug, [unquote(message), %{}])
    end
  end

  defmacro error(message) do
    quote location: :keep do
      apply(Flume.Config.logger(), :error, [unquote(message), %{}])
    end
  end

  defmacro info(message) do
    quote location: :keep do
      apply(Flume.Config.logger(), :info, [unquote(message), %{}])
    end
  end

  defmacro debug(message, opts) do
    quote location: :keep do
      apply(Flume.Config.logger(), :debug, [unquote(message), unquote(opts)])
    end
  end

  defmacro error(message, opts) do
    quote location: :keep do
      apply(Flume.Config.logger(), :error, [unquote(message), unquote(opts)])
    end
  end

  defmacro info(message, opts) do
    quote location: :keep do
      apply(Flume.Config.logger(), :info, [unquote(message), unquote(opts)])
    end
  end
end
