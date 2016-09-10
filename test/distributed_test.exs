defmodule Swarm.DistributedTests do
  use ExUnit.Case, async: false

  alias Swarm.Nodes

  @moduletag :capture_log
  @moduletag :distributed

  setup_all do
    :rand.seed(:exs64)
    {:ok, _} = :net_kernel.start([:swarm_master, :shortnames])
    :ok
  end

  test "correct redistribution of processes" do
    registry = [broadcast_period: 5, max_silent_periods: 1, permdown_period: 1_000]

    # start node1
    node1 = Nodes.start(:a, [autocluster: false, debug: true, registry: registry])
    {:ok, _} = :rpc.call(node1, Application, :ensure_all_started, [:swarm])

    # start node2
    node2 = Nodes.start(:b, [autocluster: false, debug: true, registry: registry])
    {:ok, _} = :rpc.call(node2, Application, :ensure_all_started, [:swarm])

    # give time to warm up
    :timer.sleep(1_000)

    # start 5 processes from node2 to be distributed between node1 and node2
    procs = for n <- 1..5 do
      name = :"worker#{n}"
      {:ok, pid} = :rpc.call(node2, Swarm, :register_name, [name, MyApp.Worker, :start_link, []])
      {node(pid), node2, name, pid}
    end

    # give time to sync
    :timer.sleep(1_000)

    # pull node2 from the cluster
    :rpc.call(node2, :net_kernel, :disconnect, [node1])

    # give time to sync
    :timer.sleep(1_000)

    # check to see if the processes were moved as expected
    procs
    |> Enum.filter(fn {^node2, _, _} -> true; _ -> false end)
    |> Enum.map(fn {_, name, _} ->
      pid = :rpc.call(node1, Swarm, :whereis_name, [name])
      assert node(pid) == node1
    end)

    # restore node2 to cluster
    :rpc.call(node2, :net_kernel, :connect_node, [node1])

    # give time to sync
    :timer.sleep(2_000)

    # make sure processes are back in the correct place
    procs
    |> Enum.filter(fn {^node2, _, _} -> true; _ -> false end)
    |> Enum.map(fn {_, name, _} ->
      pid = :rpc.call(node1, Swarm, :whereis_name, [name])
      assert node(pid) == node2
    end)

    node1_members = :rpc.call(node1, Swarm.Registry, :get_by_property, [:swarm_names])
    node2_members = :rpc.call(node2, Swarm.Registry, :get_by_property, [:swarm_names])
    n1ms = MapSet.new(node1_members)
    n2ms = MapSet.new(node2_members)
    empty_ms = MapSet.new([])
    assert ^empty_ms = MapSet.difference(n1ms, n2ms)
    assert length(node1_members) == 5
    assert length(node2_members) == 5

    Nodes.stop(node1)
    Nodes.stop(node2)
  end
end
