defmodule Flume.Mixfile do
  use Mix.Project

  def project do
    [
      app: :flume,
      version: "0.1.1",
      elixir: "~> 1.5",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      applications: [:redix, :logger_file_backend, :gen_stage, :poison],
      extra_applications: [:logger],
      mod: {Flume, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:redix, "~> 0.6.1"},
      {:gen_stage, "~> 0.13.0"},
      {:poison, "~> 3.1.0"},
      {:uuid, "~> 1.1.8"},
      {:logger_file_backend, "~> 0.0.4"}
    ]
  end
end
