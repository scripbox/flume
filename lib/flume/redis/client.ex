defmodule Flume.Redis.Client do
  require Flume.Logger

  alias Flume.{Config, Logger}
  alias Flume.Redis.Command

  # Redis commands
  @decr "DECR"
  @decrby "DECRBY"
  @del "DEL"
  @evalsha "EVALSHA"
  @get "GET"
  @hgetall "HGETALL"
  @incr "INCR"
  @incrby "INCRBY"
  @keys "KEYS"
  @load "LOAD"
  @lpush "LPUSH"
  @ltrim "LTRIM"
  @rpush "RPUSH"
  @lrange "LRANGE"
  @llen "LLEN"
  @lrem "LREM"
  @rpoplpush "RPOPLPUSH"
  @sadd "SADD"
  @script "SCRIPT"
  @set "SET"
  @smembers "SMEMBERS"
  @zadd "ZADD"
  @zcount "ZCOUNT"
  @zrem "ZREM"
  @zrange "ZRANGE"
  @zremrangebyscore "ZREMRANGEBYSCORE"

  @doc """
  Get all keys by key pattern.
  """
  def keys(pattern) do
    query([@keys, pattern])
  end

  @doc """
  Get all keys by key pattern.
  """
  def keys!(pattern) do
    query!([@keys, pattern])
  end

  @doc """
  Get all members of the set by key.
  """
  def smembers(key) do
    query([@smembers, key])
  end

  @doc """
  Get value of a key.
  """
  def get!(key) do
    query!([@get, key])
  end

  @doc """
  Delete the key.
  """
  def del(key) do
    query([@del, key])
  end

  @doc """
  Get all values of the hash by key.
  """
  def hgetall(key) do
    query([@hgetall, key])
  end

  @doc """
  Get all values of the hash by key.
  """
  def hgetall!(key) do
    query!([@hgetall, key])
  end

  @doc """
  Increments value of a key by 1.

  ### Examples
       iex> Flume.Redis.Client.incr("telex:test:incr")
       {:ok, 1}
  """
  def incr(key) do
    query([@incr, key])
  end

  @doc """
  Increments value of a key by given number.

  ### Examples
       iex> Flume.Redis.Client.incrby("telex:test:incrby", 10)
       {:ok, 10}
  """
  def incrby(key, count) do
    query([@incrby, key, count])
  end

  @doc """
  Decrements value of a key by 1.

  ### Examples
       iex> Flume.Redis.Client.decr("telex:test:decr")
       {:ok, -1}
  """
  def decr(key) do
    query([@decr, key])
  end

  @doc """
  Decrements value of a key by given number.

  ### Examples
       iex> Flume.Redis.Client.decrby("telex:test:decrby", 10)
       {:ok, -10}
  """
  def decrby(key, count) do
    query([@decrby, key, count])
  end

  @doc """
  Add the member to the set stored at key.
  """
  def sadd(key, value) do
    query([@sadd, key, value])
  end

  @doc """
  Add the member to the set stored at key.
  """
  def sadd!(key, value) do
    query!([@sadd, key, value])
  end

  @doc """
  Sets the value for a key
  """
  def set(key, value) do
    query([@set, key, value])
  end

  def set_nx(key, value, timeout) do
    query([@set, key, value, "NX", "PX", timeout])
  end

  @doc """
  Pushes an element at the start of a list.

  ## Examples
      iex> Flume.Redis.Client.lpush("telex:test:stack", 1)
      {:ok, 1}
  """
  def lpush(list_name, value) do
    query([@lpush, list_name, value])
  end

  def lpush_command(list_name, value) do
    [@lpush, list_name, value]
  end

  def ltrim_command(list_name, start, finish) do
    [@ltrim, list_name, start, finish]
  end

  @doc """
  Pushes an element at the end of a list.

  ## Examples
      iex> Flume.Redis.Client.rpush("telex:test:stack", 1)
      {:ok, 1}
  """
  def rpush(list_name, value) do
    query([@rpush, list_name, value])
  end

  def bulk_rpush(list_name, values) when is_list(values) do
    query([@rpush, list_name] ++ values)
  end

  def rpush_command(list_name, value) do
    [@rpush, list_name, value]
  end

  @doc """
  Returns length of the list.

  ## Examples
      iex> Flume.Redis.Client.llen!("telex:test:stack")
      0
  """
  def llen!(list_name) do
    query!([@llen, list_name])
  end

  def llen(list_name) do
    query([@llen, list_name])
  end

  @doc """
  Removes given values from the list.

  ## Examples
      iex> Flume.Redis.Client.lpush("telex:test:lb:stack", 1)
      {:ok, 1}
      iex> Flume.Redis.Client.lpush("telex:test:lb:stack", 2)
      {:ok, 2}
      iex> Flume.Redis.Client.lrem_batch("telex:test:lb:stack", [1, 2])
      {:ok, [1, 1]}
  """
  def lrem_batch(list_name, values) do
    commands =
      Enum.map(values, fn value ->
        [@lrem, list_name, 1, value]
      end)

    case pipeline(commands) do
      {:error, reason} ->
        {:error, reason}

      {:ok, reponses} ->
        success_responses =
          reponses
          |> Enum.map(fn response ->
            case response do
              value when value in [:undefined, nil] ->
                nil

              error when error in [%Redix.Error{}, %Redix.ConnectionError{}] ->
                Logger.error("#{__MODULE__} - Error running command - #{Kernel.inspect(error)}")

                nil

              value ->
                value
            end
          end)
          |> Enum.reject(&is_nil/1)

        {:ok, success_responses}
    end
  end

  @doc """
  From the given count, pops those many elements from the list and
  pushes it to different list atomically.

  ## Examples
      iex> Flume.Redis.Client.lpush("telex:test:rlb:stack", 1)
      {:ok, 1}
      iex> Flume.Redis.Client.lpush("telex:test:rlb:stack", 2)
      {:ok, 2}
      iex> Flume.Redis.Client.rpop_lpush_batch("telex:test:rlb:stack", "telex:test:rlb:new_stack", 2)
      {:ok, ["1", "2"]}
  """
  def rpop_lpush_batch(from, to, count) do
    commands =
      Enum.map(1..count, fn _ ->
        [@rpoplpush, from, to]
      end)

    case pipeline(commands) do
      {:error, reason} ->
        {:error, reason}

      {:ok, reponses} ->
        success_responses =
          reponses
          |> Enum.map(fn response ->
            case response do
              value when value in [:undefined, nil] ->
                nil

              error when error in [%Redix.Error{}, %Redix.ConnectionError{}] ->
                Logger.error("#{__MODULE__} - Error running command - #{Kernel.inspect(error)}")
                nil

              value ->
                value
            end
          end)
          |> Enum.reject(&is_nil/1)

        {:ok, success_responses}
    end
  end

  def lrem!(key, value, count \\ 1) do
    query!([@lrem, key, count, value])
  end

  def lrange!(key, range_start \\ 0, range_end \\ -1) do
    query!([@lrange, key, range_start, range_end])
  end

  def lrange(key, range_start \\ 0, range_end \\ -1) do
    query([@lrange, key, range_start, range_end])
  end

  def lrange_command(key, range_start \\ 0, range_end \\ -1) do
    [@lrange, key, range_start, range_end]
  end

  def zadd(key, score, value) do
    query([@zadd, key, score, value])
  end

  def zadd!(key, score, value) do
    query!([@zadd, key, score, value])
  end

  def bulk_zadd_command(key, scores_with_value) do
    [@zadd, key] ++ scores_with_value
  end

  def zrem!(set, member) do
    query!([@zrem, set, member])
  end

  def zrem_command(set, member) do
    [@zrem, set, member]
  end

  def zrange!(key, range_start \\ 0, range_end \\ -1) do
    query!([@zrange, key, range_start, range_end])
  end

  def zcount!(key, range_start \\ "-inf", range_end \\ "+inf") do
    query!([@zcount, key, range_start, range_end])
  end

  def zcount(key, range_start \\ "-inf", range_end \\ "+inf") do
    query([@zcount, key, range_start, range_end])
  end

  def zcount_command(key, range_start \\ "-inf", range_end \\ "+inf") do
    [@zcount, key, range_start, range_end]
  end

  def zremrangebyscore(key, range_start \\ "-inf", range_end \\ "+inf") do
    query([@zremrangebyscore, key, range_start, range_end])
  end

  def del!(key) do
    query!([@del, key])
  end

  def hset(hash, key, value), do: Command.hset(hash, key, value) |> query()

  def hscan(attr_list), do: Command.hscan(attr_list) |> pipeline()

  def hincrby(hash, key, increment \\ 1), do: Command.hincrby(hash, key, increment) |> query()

  def hdel(hash_key_list) when hash_key_list |> is_list(),
    do: hash_key_list |> Command.hdel() |> pipeline()

  def load_script!(script) do
    query!([@script, @load, script])
  end

  def evalsha_command(args) do
    [@evalsha] ++ args
  end

  def hmget(hash_key_list) when hash_key_list |> is_list(),
    do: hash_key_list |> Command.hmget() |> pipeline()

  def hmget(hash, key), do: Command.hmget(hash, key) |> query()

  def query(command) do
    Redix.command(redix_worker_name(), command, timeout: Config.redis_timeout())
  end

  def query!(command) do
    Redix.command!(redix_worker_name(), command, timeout: Config.redis_timeout())
  end

  def pipeline([]), do: []

  def pipeline(commands) when is_list(commands) do
    Redix.pipeline(redix_worker_name(), commands, timeout: Config.redis_timeout())
  end

  def transaction_pipeline([]), do: []

  def transaction_pipeline(commands) when is_list(commands) do
    Redix.transaction_pipeline(redix_worker_name(), commands, timeout: Config.redis_timeout())
  end

  # Private API
  defp random_index() do
    rem(System.unique_integer([:positive]), Config.redis_pool_size())
  end

  defp redix_worker_name do
    :"#{Flume.Redis.Supervisor.redix_worker_prefix()}_#{random_index()}"
  end
end
