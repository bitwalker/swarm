defmodule Swarm.QuorumTests do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Swarm.Distribution.StaticQuorumRing

  setup_all do
    :rand.seed(:exs64)

    Application.put_env(:swarm, :min_quorum_node_size, 3)
    restart_cluster_using_strategy(StaticQuorumRing)

    {:ok, _} = MyApp.WorkerSup.start_link()

    on_exit fn ->
      restart_cluster_using_strategy(RingStrategy)
    end

    :ok
  end

  test "block until quorum size reached" do
    quorum =
      StaticQuorumRing.create()
      |> StaticQuorumRing.add_node("node1")
      |> StaticQuorumRing.add_node("node2")

    assert StaticQuorumRing.key_to_node(quorum, :key1) == :undefined

    quorum = StaticQuorumRing.add_node(quorum, "node3")

    assert StaticQuorumRing.key_to_node(quorum, :key1) != :undefined
  end

  @tag :wip
  test "block track until quorum size reached" do
    reply_to = self()

    registration = Task.async(fn ->
      {:ok, pid1} = Swarm.register_name({:test, 1}, MyApp.WorkerSup, :register, [])

      send(reply_to, :tracked)
    end)

    refute_receive(:tracked)

    # add third node to cluster, meeting min quorum requirements
    Swarm.Cluster.spawn_node(:"node3@127.0.0.1")

    # register name should now succeed
    Task.await(registration, 5_000)

    assert_receive(:tracked)
  end

  defp restart_cluster_using_strategy(strategy) do
    Swarm.Cluster.stop()

    Application.put_env(:swarm, :distribution_strategy, strategy)

    Swarm.Cluster.spawn()

    Application.ensure_all_started(:swarm)
  end
end
