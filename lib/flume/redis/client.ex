defmodule Flume.Redis.Client do
  require Logger

  alias Flume.Config

  @pool_size Config.redis_pool_size()

  # Redis commands
  @decr "DECR"
  @decrby "DECRBY"
  @del "DEL"
  @get "GET"
  @hgetall "hgetall"
  @incr "INCR"
  @incrby "INCRBY"
  @keys "KEYS"
  @lpush "LPUSH"
  @lrange "LRANGE"
  @llen "LLEN"
  @lrem "LREM"
  @rpoplpush "RPOPLPUSH"
  @sadd "SADD"
  @smembers "SMEMBERS"
  @zadd "ZADD"
  @zrem "ZREM"
  @zrange "ZRANGE"

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
       iex> Telex.Redis.Client.incr("telex:test:incr")
       {:ok, 1}
  """
  def incr(key) do
    query([@incr, key])
  end

  @doc """
  Increments value of a key by given number.

  ### Examples
       iex> Telex.Redis.Client.incrby("telex:test:incrby", 10)
       {:ok, 10}
  """
  def incrby(key, count) do
    query([@incrby, key, count])
  end

  @doc """
  Decrements value of a key by 1.

  ### Examples
       iex> Telex.Redis.Client.decr("telex:test:decr")
       {:ok, -1}
  """
  def decr(key) do
    query([@decr, key])
  end

  @doc """
  Decrements value of a key by given number.

  ### Examples
       iex> Telex.Redis.Client.decrby("telex:test:decrby", 10)
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
  Pushes an element into a list.

  ## Examples
      iex> Telex.Redis.Client.lpush("telex:test:stack", 1)
      {:ok, 1}
  """
  def lpush(list_name, value) do
    query([@lpush, list_name, value])
  end

  @doc """
  Returns length of the list.

  ## Examples
      iex> Telex.Redis.Client.llen!("telex:test:stack")
      0
  """
  def llen!(list_name) do
    query!([@llen, list_name])
  end

  @doc """
  Removes given values from the list.

  ## Examples
      iex> Telex.Redis.Client.lpush("telex:test:lb:stack", 1)
      {:ok, 1}
      iex> Telex.Redis.Client.lpush("telex:test:lb:stack", 2)
      {:ok, 2}
      iex> Telex.Redis.Client.lrem_batch("telex:test:lb:stack", [1, 2])
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
      iex> Telex.Redis.Client.lpush("telex:test:rlb:stack", 1)
      {:ok, 1}
      iex> Telex.Redis.Client.lpush("telex:test:rlb:stack", 2)
      {:ok, 2}
      iex> Telex.Redis.Client.rpop_lpush_batch("telex:test:rlb:stack", "telex:test:rlb:new_stack", 2)
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

  def zadd(key, score, value) do
    query([@zadd, key, score, value])
  end

  def zadd!(key, score, value) do
    query!([@zadd, key, score, value])
  end

  def zrem!(set, member) do
    query!([@zrem, set, member])
  end

  def zrange!(key, range_start \\ 0, range_end \\ -1) do
    query!([@zrange, key, range_start, range_end])
  end

  def del!(key) do
    query!([@del, key])
  end

  def query(command) do
    Redix.command(redix_worker_name(), command)
  end

  def query!(command) do
    Redix.command!(redix_worker_name(), command)
  end

  def pipeline(command) do
    Redix.pipeline(redix_worker_name(), command)
  end

  # Private API
  defp random_index() do
    rem(System.unique_integer([:positive]), @pool_size)
  end

  defp redix_worker_name do
    :"#{Flume.redix_worker_prefix()}_#{random_index()}"
  end
end
