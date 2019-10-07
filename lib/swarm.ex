defmodule Swarm do
  @moduledoc """
  This is the public Elixir API for `:swarm`.
  """
  use Application

  @doc """
  Starts the Swarm application. You should not need to do this unless
  you are manually handling Swarm's application lifecycle.
  """
  def start(_type, _args) do
    Swarm.App.start_link()
  end

  @doc """
  Registers the given name to the given pid, however names
  registered this way will not be shifted when the cluster
  topology changes, but this allows you to use `:swarm` as
  a distributed process registry, including registering names
  with `{:via, :swarm, name}`.
  """
  @spec register_name(term, pid) :: :yes | :no
  def register_name(name, pid) when is_pid(pid) do
    case Swarm.Registry.register(name, pid) do
      {:ok, _} -> :yes
      _ -> :no
    end
  end

  @doc """
  Similar to register_name/2, except this version takes module/function/args
  parameters, and starts the process, registers the pid with the given name,
  and handles cluster topology changes by restarting the process on its new
  node using the given MFA.

  This version also returns an ok tuple with the pid if it registers successfully,
  or an error tuple if registration fails. You cannot use this with processes which
  are already started, it must be started by `:swarm`.

  A call to `Swarm.register_name/5` will return `{:error, :no_node_available}`
  when the configured distribution strategy returns `:undefined` as the node to
  host the named process. This indicates that there are too few nodes available to
  start a process. You can retry the name registration while waiting for nodes
  to join the cluster.

  Provide an optional `:timeout` value to limit the duration of register name calls.
  The default value is `:infinity` to block indefinitely.
  """
  @spec register_name(term, atom(), atom(), [term]) :: {:ok, pid} | {:error, term}
  @spec register_name(term, atom(), atom(), [term], non_neg_integer() | :infinity) ::
          {:ok, pid} | {:error, term}
  def register_name(name, m, f, a, timeout \\ :infinity)
  def register_name(name, m, f, a, timeout), do: Swarm.Registry.register(name, m, f, a, timeout)

  @doc """
   Either finds the named process in the swarm or registers it using the register function.
  """
  @spec whereis_or_register_name(term, atom(), atom(), [term]) :: {:ok, pid} | {:error, term}
  @spec whereis_or_register_name(term, atom(), atom(), [term], non_neg_integer() | :infinity) ::
          {:ok, pid} | {:error, term}
  def whereis_or_register_name(name, m, f, a, timeout \\ :infinity)

  def whereis_or_register_name(name, m, f, a, timeout),
    do: Swarm.Registry.whereis_or_register(name, m, f, a, timeout)

  @doc """
  Unregisters the given name from the registry.
  """
  @spec unregister_name(term) :: :ok
  defdelegate unregister_name(name), to: Swarm.Registry, as: :unregister

  @doc """
  Optimized de-registration the given name from the registry.
  """
  @spec fast_unregister_name(term) :: :ok
  defdelegate fast_unregister_name(name), to: Swarm.Registry, as: :fast_unregister

  @doc """
  Get the pid of a registered name.
  """
  @spec whereis_name(term) :: pid | :undefined
  defdelegate whereis_name(name), to: Swarm.Registry, as: :whereis

  @doc """
  Joins a process to a group
  """
  @spec join(term, pid) :: :ok
  defdelegate join(group, pid), to: Swarm.Registry

  @doc """
  Removes a process from a group
  """
  @spec leave(term, pid) :: :ok
  defdelegate leave(group, pid), to: Swarm.Registry

  @doc """
  Gets all the members of a group. Returns a list of pids.
  """
  @spec members(term()) :: [pid]
  defdelegate members(group), to: Swarm.Registry

  @doc """
  Gets a list of all registered names and their pids
  """
  @spec registered() :: [{name :: term, pid}]
  defdelegate registered, to: Swarm.Registry

  @doc """
  Publishes a message to a group. This is done via `Kernel.send/2`,
  so GenServers and the like will receive it via `handle_info/2`.
  """
  @spec publish(term, term) :: :ok
  defdelegate publish(group, msg), to: Swarm.Registry

  @doc """
  Calls each process in a group, and collects the results into a list.
  The order of the results is not guaranteed. Calls are made via `GenServer.call/2`,
  so process will need to handle a message in that format.
  """
  @spec multi_call(term, term) :: [any()]
  defdelegate multi_call(group, msg), to: Swarm.Registry

  @doc """
  Same as `multi_call/2`, except allows for a configurable timeout. The timeout
  is per-call, but since all calls are done in parallel, this is effectively the absolute
  timeout as well.
  """
  @spec multi_call(term, term, pos_integer) :: [any()]
  defdelegate multi_call(group, msg, timeout), to: Swarm.Registry

  @doc """
  This is primarily here for use by the standard library facilities for sending messages
  to a process, e.g. by `GenServer.cast/2`. It sends a message to a process by name, using
  `Kernel.send/2`.
  """
  @spec send(term, term) :: :ok
  defdelegate send(name, msg), to: Swarm.Registry
end
