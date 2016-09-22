defmodule Swarm.DistributedTests do
  use ExUnit.Case, async: false

  alias Swarm.Nodes

  @moduletag :capture_log
  @moduletag :distributed

  setup_all do
    :rand.seed(:exs64)
    {:ok, _} = :net_kernel.start([:swarm_master, :shortnames])
    Node.set_cookie(:swarm_test)
    :ok
  end

  test "correct redistribution of processes" do
    # start node1
    {:ok, node1, _node1pid} = Nodes.start(:a, [autocluster: false, debug: true])
    {:ok, _} = :rpc.call(node1, Application, :ensure_all_started, [:swarm])

    # start node2
    {:ok, node2, _node2pid} = Nodes.start(:b, [autocluster: false, debug: true])
    {:ok, _} = :rpc.call(node2, Application, :ensure_all_started, [:swarm])
    :rpc.call(node2, :net_kernel, :hidden_connect_node, [node1])

    # give time to warm up
    :timer.sleep(1_000)

    # start 5 processes from node2 to be distributed between node1 and node2
    worker_count = 50
    procs = for n <- 1..worker_count do
      name = {:"worker#{n}", n}
      #name = :"worker#{n}"
      {:ok, pid} = :rpc.call(node2, Swarm, :register_name, [name, MyApp.Worker, :start_link, []])
      {node(pid), name, pid}
    end

    #IO.puts "workers started"

    # give time to sync
    :timer.sleep(5_000)

    :rpc.call(node1, :erlang, :disconnect_node, [node2])
    :rpc.call(node2, :erlang, :disconnect_node, [node1])
    #Nodes.stop(node2)

    #IO.puts "node2 disconnected"

    # give time to sync
    :timer.sleep(5_000)

    #node1_members = :rpc.call(node1, Swarm, :registered, [], :infinity)
    #node2_members = :rpc.call(node2, Swarm, :registered, [], :infinity)
    #IO.inspect {:node1, length(node1_members), node1_members}
    #IO.inspect {:node2, length(node2_members), node2_members}

    # check to see if the processes were moved as expected
    node2procs = procs
    |> Enum.filter(fn {^node2, _, _} -> true; _ -> false end)

    #IO.inspect {:node2procs, length(node2procs)}
    node2procs
    |> Enum.map(fn {_, name, _} ->
      case :rpc.call(node1, Swarm, :whereis_name, [name]) do
        :undefined ->
          assert :undefined == node2
        pid ->
          assert node(pid) == node2
      end
    end)

    # restore node2 to cluster
    #IO.puts "node2 reconnecting"
    :rpc.call(node1, :net_kernel, :hidden_connect_node, [node2])
    #IO.puts "node2 reconnected"

    # give time to sync
    :timer.sleep(5_000)

    # make sure processes are back in the correct place
    #misplaced = procs
    procs
    |> Enum.filter(fn {^node2, _, _} -> true; _ -> false end)
    |> Enum.filter(fn {_, name, _} ->
      pid = :rpc.call(node1, Swarm, :whereis_name, [name])
      node(pid) != node2
    end)
    #IO.inspect {:misplaced, length(misplaced)}

    node1_members = :rpc.call(node1, Swarm, :registered, [], :infinity)
    node2_members = :rpc.call(node2, Swarm, :registered, [], :infinity)
    n1ms = MapSet.new(node1_members)
    n2ms = MapSet.new(node2_members)
    empty_ms = MapSet.new([])
    #IO.inspect {:node1_members, length(node1_members)}
    #IO.inspect {:node2_members, length(node2_members)}
    #IO.inspect {:union, MapSet.size(MapSet.union(n1ms, n2ms))}
    assert length(node1_members) == worker_count
    assert length(node2_members) == worker_count
    assert ^empty_ms = MapSet.difference(n1ms, n2ms)

    Nodes.stop(node1)
    Nodes.stop(node2)
  end
end
