defmodule Swarm.Logger do
  @moduledoc false
  require Logger

  @doc """
  Log a debugging message
  """
  @spec debug(String.t) :: :ok
  def debug(message), do: Logger.debug("[swarm on #{Node.self}] #{message}")

  @doc """
  Log a warning message
  """
  @spec warn(String.t) :: :ok
  def warn(message),  do: Logger.warn("[swarm on #{Node.self}] #{message}")

  @doc """
  Log an info message
  """
  @spec info(String.t) :: :ok
  def info(message), do: Logger.info("[swarm on #{Node.self}] #{message}")

  @doc """
  Log an error message
  """
  @spec error(String.t) :: :ok
  def error(message), do: Logger.error("[swarm on #{Node.self}] #{message}")
end
