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
end
