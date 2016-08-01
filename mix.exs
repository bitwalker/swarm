defmodule Swarm.Mixfile do
  use Mix.Project

  def project do
    [app: :swarm,
     version: "0.2.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     description: "Easy clustering, with registration and distribution of worker processes.",
     package: package,
     deps: deps()]
  end

  def application do
    [applications: [:logger],
     mod: {Swarm, []}]
  end

  defp deps do
    [{:ex_doc, "~> 0.13", only: :dev}]
  end

  defp package do
    [files: ["lib", "src", "mix.exs", "README.md", "LICENSE.md"],
     maintainers: ["Paul Schoenfelder"],
     licenses: ["MIT"],
     links: %{ "Gitub": "https://github.com/bitwalker/swarm" }]
  end

end
