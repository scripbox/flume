defmodule Flume.Pipeline.Control.Options do
  @option_specs [
    async: [type: :boolean, default: false],
    timeout: [type: :timeout, default: 5000],
    temporary: [type: :boolean, default: true]
  ]

  @doc """
  Returns options for pausing/resuming a pipeline
  ### Examples
      iex> Flume.Pipeline.Control.Options.sanitized_options
      {:ok, [temporary: true, timeout: 5000, async: false]}
      iex> Flume.Pipeline.Control.Options.sanitized_options([unwanted: "option", async: false, timeout: 1000])
      {:ok, [temporary: true, timeout: 1000, async: false]}
      iex> Flume.Pipeline.Control.Options.sanitized_options([temporary: true, async: true, timeout: :infinity, extra: "value"])
      {:ok, [temporary: true, timeout: :infinity, async: true]}
      iex> Flume.Pipeline.Control.Options.sanitized_options([temporary: 1, async: false])
      {:error, "expected :temporary to be a boolean, got: 1"}
      iex> Flume.Pipeline.Control.Options.sanitized_options([temporary: false, async: 0])
      {:error, "expected :async to be a boolean, got: 0"}
      iex> Flume.Pipeline.Control.Options.sanitized_options([temporary: false, async: true, timeout: -1])
      {:error, "expected :timeout to be a non-negative integer or :infinity, got: -1"}
      iex> Flume.Pipeline.Control.Options.sanitized_options([temporary: false, async: true, timeout: :inf])
      {:error, "expected :timeout to be a non-negative integer or :infinity, got: :inf"}
  """
  @type option_spec :: [
          async: boolean(),
          temporary: boolean(),
          timeout: timeout()
        ]

  @spec sanitized_options(option_spec()) :: {:ok, option_spec()} | {:error, String.t()}
  def sanitized_options(options \\ []) do
    Enum.reduce_while(@option_specs, {:ok, []}, fn {key, _} = ele, {:ok, result} ->
      case validate_option(ele, options) do
        {:ok, value} ->
          {:cont, {:ok, Keyword.put(result, key, value)}}

        {:error, _} = result ->
          {:halt, result}
      end
    end)
  end

  defp validate_option({key, spec}, options) do
    value = Keyword.get(options, key, spec[:default])
    validate_type(spec[:type], key, value)
  end

  defp validate_type(:boolean, key, value) when not is_boolean(value),
    do: {:error, "expected #{inspect(key)} to be a boolean, got: #{inspect(value)}"}

  defp validate_type(:timeout, key, value)
       when (not is_integer(value) or value < 0) and value != :infinity,
       do:
         {:error,
          "expected #{inspect(key)} to be a non-negative integer or :infinity, got: #{
            inspect(value)
          }"}

  defp validate_type(_type, _key, value), do: {:ok, value}
end
