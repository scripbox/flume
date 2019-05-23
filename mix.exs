defmodule Flume.Mixfile do
  use Mix.Project

  def project do
    [
      app: :flume,
      version: "0.1.3",
      elixir: "~> 1.6",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [coveralls: :test]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      applications: [
        :redix,
        :logger_file_backend,
        :gen_stage,
        :jason,
        :poolboy,
        :retry,
        :telemetry
      ],
      extra_applications: [:logger],
      mod: {Flume, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:redix, "~> 0.10.0"},
      {:gen_stage, "~> 0.14.0"},
      {:jason, "~> 1.1.0"},
      {:poolboy, "~> 1.5.1"},
      {:uuid, "~> 1.1.8"},
      {:logger_file_backend, "~> 0.0.10"},
      {:retry, "0.8.2"},
      {:benchee, "~> 1.0"},
      {:telemetry, "~> 0.4.0"},
      {:excoveralls, "~> 0.10.6", only: :test}
    ]
  end
end
