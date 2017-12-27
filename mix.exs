defmodule Flume.Mixfile do
  use Mix.Project

  def project do
    [
      app: :flume,
      version: "0.1.0",
      elixir: "~> 1.5",
      start_permanent: Mix.env == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      applications: [:redix],
      extra_applications: [:logger],
      mod: {Flume, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:redix, "~> 0.6.1"},
      {:gen_stage, "~> 0.12.2"}
    ]
  end
end
