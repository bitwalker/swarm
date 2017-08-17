defmodule Swarm.Distribution.StaticQuorumRing do
  @moduledoc """
  A quorum is the minimum number of nodes that a distributed cluster has to obtain in order to be
  allowed to perform an operation in a distributed system.

  Used to enforce consistent operation in a distributed system.

  ## Quorum size

  You must configure the quorum size using the `:static_quorum_size` setting:

      config :swarm,
        static_quorum_size: 5

  It defines the minimum number of nodes that must be connected in the cluster to allow process
  registration and distribution.

  If there are fewer nodes currently available than the quorum size, any calls to
  `Swarm.register_name/3` will block until enough nodes have started.

  As an example, in a 9 node cluster you would configure the `:static_quorum_size` as 5. If there
  is a network split of 4 and 5 nodes, processes on the side with 5 nodes will continue running but
  processes on the other 4 nodes will be stopped.

  Be aware that in the running 5 node cluster, no more failures can be handled, because the
  remaining cluster size would be less than 5. In the case of another failure in that 5 node
  cluster all running processes will be stopped.
  """

  use Swarm.Distribution.Strategy

  alias Swarm.Distribution.StaticQuorumRing

  defstruct [:node_count, :static_quorum_size, :ring]

  def create do
    %StaticQuorumRing{
      node_count: 0,
      static_quorum_size: Application.get_env(:swarm, :static_quorum_size, 2),
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
  def key_to_node(%StaticQuorumRing{static_quorum_size: static_quorum_size} = quorum, key) do
    case quorum.node_count do
      count when count < static_quorum_size -> :undefined
      _ -> HashRing.key_to_node(quorum.ring, key)
    end
  end
end
