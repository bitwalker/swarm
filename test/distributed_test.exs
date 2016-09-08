defmodule Swarm.DistributedTests do
  use ExUnit.Case, async: false

  alias Swarm.Nodes

  @moduletag :capture_log
  @moduletag :distributed

  setup_all do
    Logger.configure(level: :debug)
    :rand.seed(:exs64)
    {:ok, _} = :net_kernel.start([:swarm_master, :shortnames])
    :ok
  end

  test "clustering via epmd is automatic" do
    node1 = Nodes.start(:"#{:rand.uniform(1000)}", [autocluster: false])
    node2 = Nodes.start(:"#{:rand.uniform(1000)}", [autocluster: false])
    {:ok, _} = :rpc.call(node1, Application, :ensure_all_started, [:swarm])
    {:ok, _} = :rpc.call(node2, Application, :ensure_all_started, [:swarm])
    nodelist = :rpc.call(node1, Node, :list, []) -- [Node.self]
    nodelist = [node1|nodelist]
    assert [node1, node2] == nodelist

    Nodes.stop(node1)
    Nodes.stop(node2)
  end

  test "redistribution of processes is automatic" do
    pubsub = [name: Phoenix.PubSub.Test.PubSub,
              adapter: Phoenix.PubSub.PG2,
              opts: [pool_size: 1]]
    registry = [log_level: :debug, broadcast_period: 25, max_silent_periods: 3, permdown_period: 5_000]
    node1 = Nodes.start(:"#{:rand.uniform(1000)}", [autocluster: false, debug: true, pubsub: pubsub, registry: registry])
    {:ok, _} = :rpc.call(node1, Application, :ensure_all_started, [:swarm])
    IO.inspect "started #{node1}"

    node2 = Nodes.start(:"#{:rand.uniform(1000)}", [autocluster: false, debug: true, pubsub: pubsub, registry: registry])
    {:ok, _} = :rpc.call(node2, Application, :ensure_all_started, [:swarm])
    IO.inspect "started #{node2}"

    :timer.sleep(1_000)

    for n <- 1..5 do
      name = :"worker#{n}"
      {:ok, _} = :rpc.call(node1, Swarm, :register_name, [name, MyApp.Worker, :start_link, []])
    end

    :timer.sleep(1_000)

    Nodes.stop(node1)
    IO.inspect "stopped #{node1}"
    :timer.sleep(60_000)
    Nodes.stop(node2)
    IO.inspect "stopped #{node2}"

    :timer.sleep(10_000)
  end
end
