defmodule TestWithEts do
  use ExUnit.CaseTemplate

  setup _tags do
    on_exit(fn ->
      :ets.delete_all_objects(Flume.Pipeline.Event.Stats.ets_table_name())
    end)

    :ok
  end
end
