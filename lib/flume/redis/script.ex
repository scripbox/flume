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

  @spec sha(binary) :: binary
  def sha(script_name) do
    script = Path.join(scripts_dir(), "#{script_name}.lua")
    hash_sha = :crypto.hash(:sha, File.read!(script))
    Base.encode16(hash_sha, case: :lower)
  end

  @spec scripts_dir() :: binary
  defp scripts_dir, do: :code.priv_dir(:flume) |> Path.join("scripts")
end
