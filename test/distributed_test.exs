defmodule Swarm.DistributedTests do
  use ExUnit.Case, async: false

  alias Swarm.Nodes

  @moduletag timeout: 120_000
  @moduletag :capture_log
  @moduletag :distributed

  setup_all do
    :rand.seed(:exs64)
    :net_kernel.stop()
    {:ok, _} = :net_kernel.start([:swarm_master, :shortnames])
    Node.set_cookie(:swarm_test)

    on_exit(fn ->
      :net_kernel.stop()
      exclude = Keyword.get(ExUnit.configuration(), :exclude, [])

      unless :clustered in exclude do
        :net_kernel.start([:"primary@127.0.0.1"])
      end
    end)

    :ok
  end

  setup do
    whitelist = [~r/^[a-z]@.*$/]
    {:ok, node1, _node1pid} = Nodes.start(:a, debug: true, node_whitelist: whitelist)
    {:ok, node2, _node2pid} = Nodes.start(:b, debug: true, node_whitelist: whitelist)

    on_exit(fn ->
      Nodes.stop(node1)
      Nodes.stop(node2)
    end)

    [nodes: [node1, node2]]
  end

  test "correct redistribution of processes", %{nodes: [node1, node2 | _]} do
    # connect nodes
    :rpc.call(node2, :net_kernel, :connect_node, [node1])
    # start swarm
    {:ok, _} = :rpc.call(node1, Application, :ensure_all_started, [:swarm])
    {:ok, _} = :rpc.call(node2, Application, :ensure_all_started, [:swarm])
    # give time to warm up
    Process.sleep(1_000)

    # start 5 processes from node2 to be distributed between node1 and node2
    worker_count = 10

    procs =
      for n <- 1..worker_count do
        name = {:"worker#{n}", n}
        # name = :"worker#{n}"
        {:ok, pid} =
          :rpc.call(node2, Swarm, :register_name, [name, MyApp.Worker, :start_link, []])

        {node(pid), name, pid}
      end

    # IO.puts "workers started"

    # give time to sync
    Process.sleep(5_000)

    :rpc.call(node1, :net_kernel, :disconnect, [node2])
    :rpc.call(node2, :net_kernel, :disconnect, [node1])
    # Nodes.stop(node2)

    # IO.puts "node2 disconnected"

    # give time to sync
    Process.sleep(5_000)

    # node1_members = :rpc.call(node1, Swarm, :registered, [], :infinity)
    # node2_members = :rpc.call(node2, Swarm, :registered, [], :infinity)
    # node1_ms = Enum.map(node1_members, fn {k, _} -> k end)
    # node2_ms = Enum.map(node2_members, fn {k, _} -> k end)
    # IO.inspect {:node1, length(node1_members)}
    # IO.inspect {:node2, length(node2_members)}
    # missing = Enum.reject(node1_members, fn v -> MapSet.member?(node2_ms, v) end)
    # IO.inspect node2_ms -- node1_ms

    # check to see if the processes were moved as expected to node1
    node2procs =
      procs
      |> Enum.filter(fn
        {^node2, _, _} -> true
        _ -> false
      end)

    # IO.inspect {:node2procs, length(node2procs)}
    node2procs
    |> Enum.map(fn {_, name, _} ->
      case :rpc.call(node1, Swarm, :whereis_name, [name]) do
        :undefined ->
          assert :undefined == node1

        pid ->
          assert node(pid) == node1
      end
    end)

    # restore node2 to cluster
    # IO.puts "node2 reconnecting"
    :rpc.call(node1, :net_kernel, :connect_node, [node2])
    :rpc.call(node2, :net_kernel, :connect_node, [node1])
    # IO.puts "node2 reconnected"

    # give time to sync
    Process.sleep(5_000)

    # make sure processes are back in the correct place
    procs
    |> Enum.filter(fn
      {^node2, _, _} -> true
      _ -> false
    end)
    |> Enum.filter(fn {target, name, _} ->
      pid = :rpc.call(node1, Swarm, :whereis_name, [name])
      node(pid) == target
    end)

    node1_members = :rpc.call(node1, Swarm, :registered, [], :infinity)
    node2_members = :rpc.call(node2, Swarm, :registered, [], :infinity)
    n1ms = MapSet.new(node1_members)
    n2ms = MapSet.new(node2_members)
    empty_ms = MapSet.new([])
    # IO.inspect {:node1_members, length(node1_members)}
    # IO.inspect {:node2_members, length(node2_members)}
    # IO.inspect {:union, MapSet.size(MapSet.union(n1ms, n2ms))}
    assert length(node1_members) == worker_count
    assert length(node2_members) == worker_count
    assert ^empty_ms = MapSet.difference(n1ms, n2ms)
  end
end
