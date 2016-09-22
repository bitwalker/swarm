defmodule Swarm.Ring do
  @moduledoc """
  This module defines an API for creating/manipulating a hash ring.
  The internal datastructure for the hash ring is actually a gb_tree, which provides
  fast lookups for a given key on the ring.

  - The ring is a continuum of 2^32 "points", or integer values
  - Nodes are sharded into 128 points, and distributed across the ring
  - Each shard owns the keyspace below it
  - Keys are hashed and assigned a point on the ring, the node for a given
    ring is determined by finding the next highest point on the ring for a shard,
    the node that shard belongs to is then the node which owns that key.
  - If a key's hash does not have any shards above it, it belongs to the first shard,
    this mechanism is what creates the ring-like topology.
  - A node is only added to the ring if it is running Swarm
  - When nodes are added/removed from the ring, only a small subset of keys must be reassigned
  """

  @type t :: :gb_trees.tree

  @hash_range trunc(:math.pow(2, 32) - 1)

  @doc """
  Creates a new hash ring structure, with no nodes added yet
  """
  @spec new() :: __MODULE__.t
  def new(), do: :gb_trees.empty

  @doc """
  Creates a new hash ring structure, seeded with the given node,
  with an optional weight provided which determines the number of
  virtual nodes (shards) that will be assigned to it on the ring.

  The default weight for a node is 128
  """
  @spec new(node(), pos_integer) :: __MODULE__.t
  def new(node, weight \\ 128), do: add_node(new(), node, weight)

  @doc """
  Adds a node to the hash ring, with an optional weight provided which
  determines the number of virtual nodes (shards) that will be assigned to
  it on the ring.

  The default weight for a node is 128
  """
  @spec add_node(__MODULE__.t, node(), pos_integer) :: __MODULE__.t
  def add_node(ring, node, weight \\ 128) do
    Enum.reduce(1..weight, ring, fn i, acc ->
      :gb_trees.insert(:erlang.phash2("#{node}#{i}", @hash_range), node, acc)
    end)
  end

  @doc """
  Removes a node from the hash ring.
  """
  @spec remove_node(__MODULE__.t, node()) :: __MODULE__.t
  def remove_node(ring, node) do
    :gb_trees.to_list(ring)
    |> Enum.filter(fn {_key, ^node} -> false; _ -> true end)
    |> :gb_trees.from_orddict()
  end

  @doc """
  Determines which node owns the given key.
  This function assumes that the ring has been populated with at least one node.
  """
  @spec key_to_node(__MODULE__.t, term) :: node() | no_return
  def key_to_node(ring, key) do
    hash = :erlang.phash2(key, @hash_range)
    case :gb_trees.iterator_from(hash, ring) do
      [{_key, node, _, _}|_] ->
        node
      _ ->
        {_key, node} = :gb_trees.smallest(ring)
        node
    end
  end
end
