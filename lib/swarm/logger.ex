defmodule Swarm.Logger do
  @moduledoc false
  require Logger

  @doc """
  Log a debugging message
  """
  @spec debug(String.t) :: :ok
  def debug(message) do
    if log? do
      Logger.debug("(swarm) #{message}")
    else
      :ok
    end
  end

  defp log?, do: Application.get_env(:swarm, :debug, false)

end
