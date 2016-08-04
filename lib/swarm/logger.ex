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

  @doc """
  Log a warning message
  """
  @spec warn(String.t) :: :ok
  def warn(message),  do: Logger.warn("(swarm) #{message}")

  @doc """
  Log a error message
  """
  @spec error(String.t) :: :ok
  def error(message), do: Logger.error("(swarm) #{message}")

  defp log?, do: Application.get_env(:swarm, :debug, false)

end
