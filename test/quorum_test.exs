defmodule Swarm.QuorumTests do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  @node1 :"node1@127.0.0.1"
  @node2 :"node2@127.0.0.1"
  @node3 :"node3@127.0.0.1"
  @node4 :"node4@127.0.0.1"
  @node5 :"node5@127.0.0.1"
  @nodes [@node1, @node2, @node3, @node4, @node5]
  @names [{:test, 1}, {:test, 2}, {:test, 3}, {:test, 4}, {:test, 5}]

  alias Swarm.Cluster
  alias Swarm.Distribution.{Ring, StaticQuorumRing}

  setup_all do
    :rand.seed(:exs64)

    Application.put_env(:swarm, :static_quorum_size, 3)
    restart_cluster_using_strategy(StaticQuorumRing, [])

    MyApp.WorkerSup.start_link()

    on_exit(fn ->
      Application.delete_env(:swarm, :static_quorum_size)

      nodes = Application.get_env(:swarm, :nodes, [])
      restart_cluster_using_strategy(Ring, nodes)
    end)

    :ok
  end

  setup do
    on_exit(fn ->
      # stop any started nodes after each test
      @nodes
      |> Enum.map(&Task.async(fn -> Cluster.stop_node(&1) end))
      |> Enum.map(&Task.await(&1, 30_000))
    end)
  end

  describe "without quorum cluster" do
    setup [:form_two_node_cluster]

    test "should error on name registration" do
      assert {:error, :no_node_available} =
               register_name(@node1, {:test, 1}, MyApp.WorkerSup, :register, [])

      Enum.each([@node1, @node2], fn node ->
        assert whereis_name(node, {:test, 1}) == :undefined
        assert get_registry(node) == []
      end)
    end

    test "should optionally timeout a track call" do
      case register_name(@node1, {:test, 1}, MyApp.WorkerSup, :register, [], 0) do
        {:error, {:EXIT, {:timeout, _}}} -> :ok
        reply -> flunk("expected timeout, instead received: #{inspect(reply)}")
      end
    end
  end

  describe "with quorum cluster" do
    setup [:form_three_node_cluster]

    test "should immediately start registered process" do
      assert {:ok, _pid} = register_name(@node1, {:test, 1}, MyApp.WorkerSup, :register, [])
      assert :ok = unregister_name(@node1, {:test, 1})
    end

    test "should kill process after topology change results in too few nodes to host" do
      {:ok, pid} = register_name(@node1, {:test, 1}, MyApp.WorkerSup, :register, [])

      ref = Process.monitor(pid)

      # stopping a node means not enough nodes for a quorum, running processes must be stopped
      Cluster.stop_node(@node3)

      assert_receive {:DOWN, ^ref, _, _, _}

      :timer.sleep(1_000)

      Enum.each([@node1, @node2], fn node ->
        assert whereis_name(node, {:test, 1}) == :undefined
        assert get_registry(node) == []
      end)
    end

    test "should kill all processes after topology change results in too few nodes to host" do
      refs = start_named_processes()

      :timer.sleep(1_000)

      # stopping one node means not enough nodes for a quorum, running processes must be stopped
      Cluster.stop_node(@node3)

      :timer.sleep(1_000)

      # ensure all processes have been stopped
      Enum.each(refs, fn ref ->
        assert_receive {:DOWN, ^ref, _, _, _}
      end)

      # ensure all nodes have an empty process registry
      Enum.each(@names, fn name ->
        assert whereis_name(@node1, name) == :undefined
        assert whereis_name(@node2, name) == :undefined
        assert get_registry(@node2) == []
        assert get_registry(@node2) == []
      end)
    end

    test "should unregister name" do
      {:ok, _pid} = register_name(@node1, {:test, 1}, MyApp.WorkerSup, :register, [])

      :timer.sleep(1_000)

      assert :ok = unregister_name(@node1, {:test, 1})

      :timer.sleep(1_000)

      Enum.each([@node1, @node2, @node3], fn node ->
        assert whereis_name(node, {:test, 1}) == :undefined
        assert get_registry(node) == []
      end)
    end
  end

  describe "net split" do
    setup [:form_five_node_cluster]

    test "should redistribute processes from smaller to larger partition" do
      # start worker for each name
      Enum.each(@names, fn name ->
        {:ok, _pid} = register_name(@node1, name, MyApp.WorkerSup, :register, [])
      end)

      :timer.sleep(1_000)

      # simulate net split (1, 2, 3) and (4, 5)
      simulate_disconnect([@node1, @node2, @node3], [@node4, @node5])

      :timer.sleep(1_000)

      # ensure processes are redistributed onto nodes 1, 2, or 3 (quorum)
      @names
      |> Enum.map(&whereis_name(@node1, &1))
      |> Enum.each(fn pid ->
        refute pid == :undefined

        case node(pid) do
          @node4 -> flunk("process still running on node4")
          @node5 -> flunk("process still running on node5")
          _ -> :ok
        end
      end)

      Enum.each(@names, fn name ->
        assert :ok = unregister_name(@node1, name)
      end)
    end

    # simulate a disconnect between the two node partitions
    defp simulate_disconnect(lpartition, rpartition) do
      Enum.each(lpartition, fn lnode ->
        Enum.each(rpartition, fn rnode ->
          send({Swarm.Tracker, lnode}, {:nodedown, rnode, nil})
          send({Swarm.Tracker, rnode}, {:nodedown, lnode, nil})
        end)
      end)
    end
  end

  defp get_registry(node) do
    :rpc.call(node, Swarm, :registered, [], :infinity)
  end

  defp register_name(node, name, m, f, a, timeout \\ :infinity)

  defp register_name(node, name, m, f, a, timeout) do
    case :rpc.call(node, Swarm, :register_name, [name, m, f, a, timeout], :infinity) do
      {:badrpc, reason} -> {:error, reason}
      reply -> reply
    end
  end

  defp unregister_name(node, name) do
    :rpc.call(node, Swarm, :unregister_name, [name], :infinity)
  end

  defp whereis_name(node, name) do
    :rpc.call(node, Swarm, :whereis_name, [name], :infinity)
  end

  defp form_two_node_cluster(_context) do
    with {:ok, _node1} <- Cluster.spawn_node(@node1),
         {:ok, _node2} <- Cluster.spawn_node(@node2) do
      :ok
    end
  end

  defp form_three_node_cluster(_context) do
    with {:ok, _node1} <- Cluster.spawn_node(@node1),
         {:ok, _node2} <- Cluster.spawn_node(@node2),
         {:ok, _node3} <- Cluster.spawn_node(@node3) do
      Process.sleep(2000)
      :ok
    end
  end

  defp form_five_node_cluster(_context) do
    with {:ok, _node1} <- Cluster.spawn_node(@node1),
         {:ok, _node2} <- Cluster.spawn_node(@node2),
         {:ok, _node3} <- Cluster.spawn_node(@node3),
         {:ok, _node4} <- Cluster.spawn_node(@node4),
         {:ok, _node5} <- Cluster.spawn_node(@node5) do
      Process.sleep(2000)
      :ok
    end
  end

  # start worker for each name
  def start_named_processes do
    Enum.map(@names, fn name ->
      with {:ok, pid} <- register_name(@node1, name, MyApp.WorkerSup, :register, []) do
        Process.monitor(pid)
      end
    end)
  end

  defp restart_cluster_using_strategy(strategy, nodes) do
    Cluster.stop()

    Application.put_env(:swarm, :distribution_strategy, strategy)
    Application.stop(:swarm)

    Cluster.spawn(nodes)

    Application.ensure_all_started(:swarm)
  end
end
