defmodule Flume.UtilsTest do
  use ExUnit.Case

  alias Flume.Utils

  describe "safe_apply/2" do
    test "returns result correctly" do
      response = "Test"
      result = Utils.safe_apply(fn -> response end, 1000)
      assert {:ok, response} == result
    end

    test "allows processes to crash safely" do
      error_msg = "Test"
      result = Utils.safe_apply(fn -> raise error_msg end, 1000)
      assert {:exit, {foo, _stack}} = result
      assert error_msg == foo.message
    end

    test "times out processes" do
      result = Utils.safe_apply(fn -> Process.sleep(1200) end, 1000)
      assert {:timeout, _} = result
    end
  end
end
