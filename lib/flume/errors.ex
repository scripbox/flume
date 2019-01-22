defmodule Flume.Errors do
  defmodule InvalidConfiguration do
    defexception [:message]

    def exception(key) do
      msg = "Invalid configuration for #{key}"
      %__MODULE__{message: msg}
    end
  end
end
