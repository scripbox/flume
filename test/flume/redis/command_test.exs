defmodule Flume.Redis.CommandTest do
  use Flume.TestWithRedis

  alias Flume.Redis.Command

  describe "hdel/1" do
    test "returns list of HDEL commands" do
      hash_key_list = [{"hash_1", "key_1"}, {"hash_1", "key_2"}, {"hash_2", "key_1"}]

      assert Command.hdel(hash_key_list) == [
               ["HDEL", "hash_1", "key_1", "key_2"],
               ["HDEL", "hash_2", "key_1"]
             ]
    end
  end

  describe "hdel/2" do
    test "returns HDEL command for list of keys to be deleted from a hash" do
      hash = "hash_1"
      keys = ["key_1", "key_2", "key_3"]

      assert Command.hdel(hash, keys) == ["HDEL", "hash_1", "key_1", "key_2", "key_3"]
    end

    test "returns HDEL command for a key to be deleted from a hash" do
      assert Command.hdel("hash", "key") == ["HDEL", "hash", "key"]
    end
  end

  describe "hscan/1" do
    test "returns list of HSCAN commands" do
      expected_commands = [
        ["HSCAN", "hash_1", 0, "MATCH", "xyz"],
        ["HSCAN", "hash_2", 0],
        ["HSCAN", "hash_3", 0, "MATCH", "abc"]
      ]

      attr_list = [
        ["hash_1", 0, "xyz"],
        ["hash_2", 0],
        ["hash_3", 0, "abc"]
      ]

      assert Command.hscan(attr_list) == expected_commands
    end
  end

  describe "hmget/1" do
    test "returns list of HMGET commands" do
      expected_commands = [
        ["HMGET", "hash_1", "abc", "xyz"],
        ["HMGET", "hash_2", "xyz"],
        ["HMGET", "hash_3", "abc"]
      ]

      attr_list = [
        {"hash_1", ["abc", "xyz"]},
        {"hash_2", "xyz"},
        {"hash_3", ["abc"]}
      ]

      assert Command.hmget(attr_list) == expected_commands
    end
  end
end
