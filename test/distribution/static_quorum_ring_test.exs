defmodule Swarm.Distribution.StaticQuorumRingTests do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Swarm.Distribution.StaticQuorumRing

  test "key to node should return `:undefined` until quorum size reached" do
    quorum =
      StaticQuorumRing.create()
      |> StaticQuorumRing.add_node("node1")

    assert StaticQuorumRing.key_to_node(quorum, :key1) == :undefined

    quorum = StaticQuorumRing.add_node(quorum, "node2")
    assert StaticQuorumRing.key_to_node(quorum, :key1) != :undefined

    quorum = StaticQuorumRing.add_node(quorum, "node3")
    assert StaticQuorumRing.key_to_node(quorum, :key1) != :undefined
  end

  test "quorum size should be set by binary setting" do
    static_quorum_size = Application.get_env(:swarm, :static_quorum_size)
    Application.put_env(:swarm, :static_quorum_size, "5")

    assert StaticQuorumRing.create() == %StaticQuorumRing{
             ring: %HashRing{},
             static_quorum_size: 5
           }

    Application.put_env(:swarm, :static_quorum_size, static_quorum_size)
  end

  test "creating StaticQuorumRing should raise if the setting is not a positive integer" do
    static_quorum_size = Application.get_env(:swarm, :static_quorum_size)

    Application.put_env(:swarm, :static_quorum_size, 0)

    assert_raise(
      RuntimeError,
      "config :static_quorum_size should be a positive integer",
      &StaticQuorumRing.create/0
    )

    Application.put_env(:swarm, :static_quorum_size, {:strange, :tuple})

    assert_raise(
      RuntimeError,
      "config :static_quorum_size should be a positive integer",
      &StaticQuorumRing.create/0
    )

    Application.put_env(:swarm, :static_quorum_size, 0.5)

    assert_raise(
      RuntimeError,
      "config :static_quorum_size should be a positive integer",
      &StaticQuorumRing.create/0
    )

    Application.put_env(:swarm, :static_quorum_size, "fake")

    assert_raise(
      RuntimeError,
      "config :static_quorum_size should be a positive integer",
      &StaticQuorumRing.create/0
    )

    Application.put_env(:swarm, :static_quorum_size, static_quorum_size)
  end
end
