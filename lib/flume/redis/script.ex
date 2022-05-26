defmodule Flume.Redis.Script do
  @moduledoc """
  Provides helpers to load lua scripts into redis and calculate sha1
  """

  alias Flume.Redis.Client

  @spec load() :: :ok
  def load do
    dir = scripts_dir()

    for file <- File.ls!(dir), path = Path.join(dir, file), script = File.read!(path) do
      Client.load_script!(script)
    end

    :ok
  end

  @spec compile(binary) :: {binary, binary}
  def compile(script_name) do
    script =
      Path.join(scripts_dir(), "#{script_name}.lua")
      |> File.read!()

    hash_sha = :crypto.hash(:sha, script)
    {script, Base.encode16(hash_sha, case: :lower)}
  end

  @spec eval({binary, binary}, List.t()) :: {:ok, term} | {:error, term}
  def eval({script, sha}, arguments) do
    result =
      Client.evalsha_command([sha | arguments])
      |> Client.query()

    case result do
      {:error, %Redix.Error{message: "NOSCRIPT" <> _}} ->
        Client.eval_command([script | arguments])
        |> Client.query()

      result ->
        result
    end
  end

  @spec scripts_dir() :: binary
  defp scripts_dir, do: :code.priv_dir(:flume) |> Path.join("scripts")
end
