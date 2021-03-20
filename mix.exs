# Since we depend on gen_statem, an OTP 19 construct
# Warn if someone depends on this in <19
otp_release =
  String.split("#{:erlang.system_info(:otp_release)}", ".")
  |> List.first()
  |> String.to_integer()

if otp_release < 19 do
  IO.warn("Swarm requires Erlang/OTP 19 or greater", [])
end

defmodule Swarm.Mixfile do
  use Mix.Project

  @source_url "https://github.com/bitwalker/swarm"
  @version "3.4.0"

  def project do
    [
      app: :swarm,
      version: @version,
      elixir: "~> 1.6",
      elixirc_paths: elixirc_paths(Mix.env()),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      package: package(),
      docs: docs(),
      deps: deps(),
      aliases: aliases(),
      dialyzer: dialyzer()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Swarm, []}
    ]
  end

  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:dialyxir, "~> 0.3", only: :dev},
      {:benchee, "~> 0.4", only: :dev},
      {:porcelain, "~> 2.0", only: [:dev, :test]},
      {:libring, "~> 1.0"},
      {:gen_state_machine, "~> 2.0"}
    ]
  end

  defp package do
    [
      description: """
        A fast, multi-master, distributed global process registry, with
        automatic distribution of worker processes.
      """,
      files: ["lib", "src", "mix.exs", "README.md", "LICENSE.md", "CHANGELOG.md"],
      maintainers: ["Paul Schoenfelder"],
      licenses: ["MIT"],
      links: %{
        Changelog: "https:hexdocs.pm/swarm/changelog.html",
        GitHub: @source_url
      }
    ]
  end

  defp docs do
    [
      extras: ["CHANGELOG.md", "README.md"],
      main: "readme",
      source_url: @source_url,
      source_ref: @version,
      formatter_opts: [gfm: true],
      formatters: ["html"]
    ]
  end

  defp aliases do
    if System.get_env("SWARM_TEST_DEBUG") do
      [test: "test --no-start --trace"]
    else
      [test: "test --no-start"]
    end
  end

  defp dialyzer do
    [
      plt_add_apps: [:inets],
      plt_add_deps: :transitive,
      flags: ~w(-Wunmatched_returns -Werror_handling -Wrace_conditions -Wunderspecs)
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
