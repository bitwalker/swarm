defmodule Swarm.Cluster.Epmd do
  @moduledoc """
  This clustering strategy relies on Erlang's built-in distribution protocol
  """
  def start_link(), do: :ignore
end
