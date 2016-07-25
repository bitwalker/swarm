defmodule Distable.Logger do
  @moduledoc false
  require Logger

  @doc """
  Log a debugging message
  """
  @spec debug(String.t) :: :ok
  def debug(message) do
    if log? do
      Logger.debug("(distable) #{message}")
    else
      :ok
    end
  end

  defp log?, do: Application.get_env(:distable, :debug, false)

end
