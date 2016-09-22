defmodule Swarm.Logger do
  @moduledoc false
  require Logger

  @doc """
  Log a debugging message
  """
  @spec debug(String.t) :: :ok
  def debug(message) do
    if log? do
      Logger.debug("[swarm on #{Node.self}] #{message}")
    else
      :ok
    end
  end

  @doc """
  Log a warning message
  """
  @spec warn(String.t) :: :ok
  def warn(message),  do: Logger.warn("[swarm on #{Node.self}] #{message}")

  @doc """
  Log a error message
  """
  @spec error(String.t) :: :ok
  def error(message), do: Logger.error("[swarm on #{Node.self}] #{message}")

  defp log?, do: Application.get_env(:swarm, :debug, false)

end
