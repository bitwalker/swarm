defmodule Swarm.Distribution.Strategy do
  @moduledoc """
  This module implements the interface for custom distribution strategies.
  The default strategy used by Swarm is a consistent hash ring implemented
  via the `libring` library.

  Custom strategies are expected to return a datastructure or pid which will be
  passed along to any functions which need to manipulate the current distribution state.
  This can be either a plain datastructure (as is the case with the libring-based strategy),
  or a pid which your strategy module then uses to call a process in your own supervision tree.

  For efficiency reasons, it is highly recommended to use plain datastructures rather than a
  process for storing the distribution state, because it has the potential to become a bottleneck otherwise,
  however this is really up to the needs of your situation, just know that you can go either way.
  """
  alias Swarm.Distribution.Ring, as: RingStrategy

  defmacro __using__(_) do
    quote do
      @behaviour Swarm.Distribution.Strategy
    end
  end

  @type reason :: String.t
  @type strategy :: term
  @type weight :: pos_integer
  @type nodelist :: [node() | {node(), weight}]
  @type key :: term

  @type t :: strategy

  @callback create() :: strategy | {:error, reason}
  @callback add_node(strategy, node) :: strategy | {:error, reason}
  @callback add_node(strategy, node, weight) :: strategy | {:error, reason}
  @callback add_nodes(strategy, nodelist) :: strategy | {:error, reason}
  @callback remove_node(strategy, node) :: strategy | {:error, reason}
  @callback key_to_node(strategy, key) :: node() | :undefined

  def create(), do: strategy_module().create()
  def create(node), do: strategy_module().add_node(create(), node)

  @doc """
  Adds a node to the state of the current distribution strategy.
  """
  def add_node(strategy, node) do
    strategy_module().add_node(strategy, node)
  end

  @doc """
  Adds a node to the state of the current distribution strategy,
  and give it a specific weighting relative to other nodes.
  """
  def add_node(strategy, node, weight) do
    strategy_module().add_node(strategy, node, weight)
  end

  @doc """
  Adds a list of nodes to the state of the current distribution strategy.
  The node list can be composed of both node names (atoms) or tuples containing
  a node name and a weight for that node.
  """
  def add_nodes(strategy, nodes) do
    strategy_module().add_nodes(strategy, nodes)
  end

  @doc """
  Removes a node from the state of the current distribution strategy.
  """
  def remove_node(strategy, node) do
    strategy_module().remove_node(strategy, node)
  end

  @doc """
  Maps a key to a specific node via the current distribution strategy.
  """
  def key_to_node(strategy, node) do
    strategy_module().key_to_node(strategy, node)
  end

  defp strategy_module(), do: Application.get_env(:swarm, :distribution_strategy, RingStrategy)
end
