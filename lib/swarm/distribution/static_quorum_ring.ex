defmodule Swarm.Distribution.StaticQuorumRing do
  @moduledoc """
  A quorum is the minimum number of nodes that a distributed cluster has to obtain in order to be
  allowed to perform an operation in a distributed system.

  Used to enforce consistent operation in a distributed system.
  """

  use Swarm.Distribution.Strategy

  alias Swarm.Distribution.StaticQuorumRing

  defstruct [:node_count, :min_node_count, :ring]

  def create do
    %StaticQuorumRing{
      node_count: 0,
      min_node_count: Application.get_env(:swarm, :min_quorum_node_size, 2),
      ring: HashRing.new(),
    }
  end

  def add_node(quorum, node) do
    %StaticQuorumRing{quorum |
      node_count: quorum.node_count + 1,
      ring: HashRing.add_node(quorum.ring, node),
    }
  end

  def add_node(quorum, node, weight) do
    %StaticQuorumRing{quorum |
      node_count: quorum.node_count + 1,
      ring: HashRing.add_node(quorum.ring, node, weight),
    }
  end

  def add_nodes(quorum, nodes) do
    %StaticQuorumRing{quorum |
      node_count: quorum.node_count + length(nodes),
      ring: HashRing.add_nodes(quorum.ring, nodes),
    }
  end

  def remove_node(quorum, node) do
    %StaticQuorumRing{quorum |
      node_count: quorum.node_count - 1,
      ring: HashRing.remove_node(quorum.ring, node),
    }
  end

  @doc """
  Maps a key to a specific node via the current distribution strategy.

  If the available nodes in the cluster are fewer than the minimum node count it returns `:undefined`.
  """
  def key_to_node(%StaticQuorumRing{min_node_count: min_node_count} = quorum, key) do
    case quorum.node_count do
      count when count < min_node_count -> :undefined
      _ -> HashRing.key_to_node(quorum.ring, key)
    end
  end
end
