defmodule Distable do
  @moduledoc """
  This is the public Elixir API for `:distable`.
  """
  use Application

  def start(_type, _args) do
    Distable.Supervisor.start_link()
  end

  @doc """
  Registers a name to a process started by the provided
  module/function/args. If the MFA does not start a process,
  an error will be returned.
  """
  @spec register(term, atom(), atom(), [term]) :: {:ok, pid} | {:error, term}
  def register(name, module, function, args)
    when is_atom(module) and is_atom(function) and is_list(args) do
    Distable.Tracker.register(name, {module, function, args})
  end
  @doc """
  Unregisters a name.
  """
  @spec unregister(term) :: :ok
  def unregister(name) do
    Distable.Tracker.unregister(name)
  end
  @doc """
  Add metadata to a registered process.
  """
  @spec register_property(pid, term) :: :ok
  def register_property(pid, prop) do
    Distable.Tracker.register_property(pid, prop)
  end
  @doc """
  Get the pid of a registered name.
  """
  @spec whereis(term) :: pid | :undefined
  def whereis(name) do
    Distable.Tracker.whereis(name)
  end
  @doc """
  Get a list of pids which have the given property in their
  metadata.
  """
  @spec get_by_property(term) :: [pid]
  def get_by_property(prop) do
    Distable.Tracker.get_by_property(prop)
  end
  @doc """
  Call the server associated with a given name.
  Use like `GenServer.call/3`
  """
  @spec call(term, term) :: term | {:error, term}
  @spec call(term, term, pos_integer) :: term | {:error, term}
  def call(name, message, timeout \\ 5_000) do
    Distable.Tracker.call(name, message, timeout)
  end
  @doc """
  Cast a message to a server associated with the given name.
  Use like `GenServer.cast/2`
  """
  @spec cast(term, term) :: :ok
  def cast(name, message) do
    Distable.Tracker.cast(name, message)
  end
end
