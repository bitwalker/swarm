defmodule Swarm.Logger do
  @moduledoc false

  @doc """
  Formats a log message to include info on which node swarm is running on.
  """
  @spec format(String.t()) :: String.t()
  def format(message), do: "[swarm on #{Node.self()}] #{message}"
end
