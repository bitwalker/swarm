defmodule Swarm.QuorumTests do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Swarm.Distribution.{Ring,StaticQuorumRing}

  setup_all do
    :rand.seed(:exs64)

    Application.put_env(:swarm, :min_quorum_node_size, 3)
    restart_cluster_using_strategy(StaticQuorumRing)

    {:ok, _} = MyApp.WorkerSup.start_link()

    on_exit fn ->
      restart_cluster_using_strategy(Ring)
    end

    :ok
  end

  setup do
    on_exit fn ->
      Swarm.Cluster.stop_node(:"node3@127.0.0.1")
    end
  end

  test "block name registration until quorum size reached" do
    reply_to = self()

    registration = Task.async(fn ->
      {:ok, _pid} = Swarm.register_name({:test, 1}, MyApp.WorkerSup, :register, [])

      send(reply_to, :tracked)
    end)

    refute_receive(:tracked)

    # add third node to cluster, meeting min quorum requirements
    {:ok, _node} = Swarm.Cluster.spawn_node(:"node3@127.0.0.1")

    # register name should now succeed
    Task.await(registration, 5_000)

    assert_receive(:tracked)
  end

  describe "with quorum cluster" do
    setup [:start_third_node]

    test "kill process after topology change results in no available node to host", %{node3: node3} do
      {:ok, pid} = Swarm.register_name({:test, 1}, MyApp.WorkerSup, :register, [])

      ref = Process.monitor(pid)

      # stopping node3 means not enough needs for a quorum, running processes must be stopped
      Swarm.Cluster.stop_node(node3)

      assert_receive {:DOWN, ^ref, _, _, _}
    end

    test "restart a killed process after topology change restores min quorum", %{node3: node3} do
      {:ok, pid} = Swarm.register_name({:test, 1}, MyApp.WorkerSup, :register, [])

      ref = Process.monitor(pid)

      # stopping node3 means not enough needs for a quorum, running processes must be stopped
      Swarm.Cluster.stop_node(node3)
      assert_receive {:DOWN, ^ref, _, _, _}

      # restore third node to cluster, meeting min quorum requirements
      {:ok, _node3} = Swarm.Cluster.spawn_node(:"node3@127.0.0.1")

      assert Swarm.whereis_name({:test, 1}) != :undefined
    end

    # add third node to cluster, meeting min quorum requirements
    defp start_third_node(_context) do
      {:ok, node3} = Swarm.Cluster.spawn_node(:"node3@127.0.0.1")

      [node3: node3]
    end
  end

  defp restart_cluster_using_strategy(strategy) do
    Swarm.Cluster.stop()

    Application.put_env(:swarm, :distribution_strategy, strategy)

    Application.stop(:swarm)

    Swarm.Cluster.spawn()

    Application.ensure_all_started(:swarm)
  end
end
