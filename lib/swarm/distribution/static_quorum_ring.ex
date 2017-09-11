defmodule Swarm.Distribution.StaticQuorumRing do
  @moduledoc """
  A quorum is the minimum number of nodes that a distributed cluster has to
  obtain in order to be allowed to perform an operation. This can be used to
  enforce consistent operation in a distributed system.

  ## Quorum size

  You must configure this distribution strategy and specify its minimum quorum
  size:

      config :swarm,
        distribution_strategy: Swarm.Distribution.StaticQuorumRing,
        static_quorum_size: 5

  It defines the minimum number of nodes that must be connected in the cluster
  to allow process registration and distribution.

  If there are fewer nodes currently available than the quorum size, any calls
  to `Swarm.register_name/5` will return `{:error, :no_node_available}` until
  enough nodes have started.

  You can configure the `:kernel` application to wait for cluster formation
  before starting your application during node start up. The
  `sync_nodes_optional` configuration specifies which nodes to attempt to
  connect to within the `sync_nodes_timeout` window, defined in milliseconds,
  before continuing with startup. There is also a `sync_nodes_mandatory` setting
  which can be used to enforce all nodes are connected within the timeout window
  or else the node terminates.

      config :kernel,
        sync_nodes_optional: [:"node1@192.168.1.1", :"node2@192.168.1.2"],
        sync_nodes_timeout: 60_000

  The `sync_nodes_timeout` can be configured as `:infinity` to wait indefinitely
  for all nodes to connect. All involved nodes must have the same value for
  `sync_nodes_timeout`.

  ### Example

  In a 9 node cluster you would configure the `:static_quorum_size` as 5. During
  a network split of 4 and 5 nodes, processes on the side with 5 nodes
  will continue running, whereas processes on the other 4 nodes will be stopped.

  Be aware that in the running 5 node cluster, no more failures can be handled
  because the remaining cluster size would be less than the required 5 node
  minimum. All running processes would be stopped in the case of another single
  node failure.
  """

  use Swarm.Distribution.Strategy

  alias Swarm.Distribution.StaticQuorumRing

  defstruct [:static_quorum_size, :ring]

  def create do
    %StaticQuorumRing{
      static_quorum_size: Application.get_env(:swarm, :static_quorum_size, 2),
      ring: HashRing.new(),
    }
  end

  def add_node(quorum, node) do
    %StaticQuorumRing{quorum |
      ring: HashRing.add_node(quorum.ring, node),
    }
  end

  def add_node(quorum, node, weight) do
    %StaticQuorumRing{quorum |
      ring: HashRing.add_node(quorum.ring, node, weight),
    }
  end

  def add_nodes(quorum, nodes) do
    %StaticQuorumRing{quorum |
      ring: HashRing.add_nodes(quorum.ring, nodes),
    }
  end

  def remove_node(quorum, node) do
    %StaticQuorumRing{quorum |
      ring: HashRing.remove_node(quorum.ring, node),
    }
  end

  @doc """
  Maps a key to a specific node via the current distribution strategy.

  If the available nodes in the cluster are fewer than the minimum node count it returns `:undefined`.
  """
  def key_to_node(%StaticQuorumRing{static_quorum_size: static_quorum_size, ring: ring}, key) do
    case length(ring.nodes) do
      node_count when node_count < static_quorum_size -> :undefined
      _ -> HashRing.key_to_node(ring, key)
    end
  end
end
