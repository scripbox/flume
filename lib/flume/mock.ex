defmodule Flume.Mock do
  defmacro __using__(_) do
    quote do
      setup _mock do
        Application.put_env(:flume, :mock, true)

        on_exit(fn ->
          Application.put_env(:flume, :mock, false)
        end)

        :ok
      end
    end
  end

  defmacro with_flume_mock(do: yield) do
    quote do
      Application.put_env(:flume, :mock, true)

      on_exit(fn ->
        Application.put_env(:flume, :mock, false)
      end)

      unquote(yield)
    end
  end
end
