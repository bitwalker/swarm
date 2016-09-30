defmodule Swarm.Mixfile do
  use Mix.Project

  def project do
    [app: :swarm,
     version: "2.0.0",
     elixir: "~> 1.3",
     elixirc_paths: elixirc_paths(Mix.env),
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     description: "Easy clustering, with registration and distribution of worker processes.",
     package: package,
     docs: docs(),
     deps: deps(),
     aliases: aliases(),
     dialyzer: [
       plt_add_apps: [:inets],
       plt_add_deps: :transitive,
       flags: ~w(-Wunmatched_returns -Werror_handling -Wrace_conditions -Wunderspecs)
     ]]
  end

  def application do
    [applications: [:logger, :crypto, :libring],
     mod: {Swarm, []}]
  end

  defp deps do
    [{:ex_doc, "~> 0.13", only: :dev},
     {:dialyxir, "~> 0.3", only: :dev},
     {:benchee, "~> 0.4", only: :dev},
     {:porcelain, "~> 2.0", only: [:dev, :test]},
     {:libring, "~> 0.1"}]
  end

  defp package do
    [files: ["lib", "src", "mix.exs", "README.md", "LICENSE.md"],
     maintainers: ["Paul Schoenfelder"],
     licenses: ["MIT"],
     links: %{ "Github": "https://github.com/bitwalker/swarm" }]
  end

  defp docs do
    [main: "readme",
     formatter_opts: [gfm: true],
     extras: [
       "README.md"
     ]]
  end

  defp aliases() do
    ["test": "test --no-start --trace --seed=0"]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
