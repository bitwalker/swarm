defmodule Swarm.Cluster.Epmd do
  @moduledoc """
  This clustering strategy is effectively a no-op
  """
  use GenServer

  def start_link(), do: :ignore
  def init(_) do
    {:ok, []}
  end
end
