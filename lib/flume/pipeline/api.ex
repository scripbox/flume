defmodule Flume.Pipeline.API do
  @callback pause(String.t(), keyword()) :: :ok | {:error, String.t()}
  @callback resume(String.t(), keyword()) :: :ok | {:error, String.t()}
end
