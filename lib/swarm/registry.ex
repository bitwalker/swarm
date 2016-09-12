defmodule Swarm.Registry do
  @moduledoc """
  This module defines an implementation of Phoenix.Tracker which
  manages synchronizing the process registry between nodes in the cluster.

  There are a couple of key points about this implementation:

  - Presence changes are written to an ETS table which is used for
    conflict resolution, and as a source of truth for registry lookups.
  - Conflict resolution is last-write wins, older registrations are removed
    and their processes are sent the `{:swarm, :die}` message. This is only
    done for registrations done via `Swarm.register_name/4`.
  """
  import Swarm.Logger

  @behaviour Phoenix.Tracker

  # Registry API

  @spec register_name(term, pid, Enum.t, [any()]) :: {:ok, pid} | {:error, {:already_registered, pid}}
  def register_name(name, pid, meta, opts) do
    case get_in(opts, [:checked]) do
      false ->
        do_register(name, pid, meta)
      _ ->
        case :ets.match_object(:swarm_registry, {{name, :'$1'}, :'$2'}) do
          [] ->
            do_register(name, pid, meta)
          dups ->
            [pid|_] = Enum.sort(Enum.map(dups, fn {{_,p},_} -> p end))
            {:error, {:already_registered, pid}}
        end
    end
  end

  @spec register_name(term, module, atom, [any()], [any()]) :: {:ok, pid} | {:error, {:already_registered, pid}}
  def register_name(name, module, fun, args, opts) do
    try do
      node = Node.self
      case Swarm.Ring.node_for_key(name) do
        ^node ->
          create? = case get_in(opts, [:checked]) do
            false -> true
            _ ->
              case :ets.match_object(:swarm_registry, {{name, :'$1'}, :'$2'}) do
                [] -> true
                dups ->
                  [pid|_] = Enum.sort(Enum.map(dups, fn {{_,p},_} -> p end))
                  {:error, {:already_registered, pid}}
              end
          end
          case create? do
            {:error, _} = err -> err
            true ->
              case apply(module, fun, args) do
                {:ok, pid} -> do_register(name, pid, [mfa: {module, fun, args}])
                other -> {:error, {:bad_return, other}}
              end
          end
        other_node ->
          :rpc.call(other_node, __MODULE__, :register_name, [name, module, fun, args, opts])
      end
    catch
      {_, err} ->
        {:error, err}
    end
  end

  @spec unregister_name(name :: term) :: :ok
  def unregister_name(name) do
    case get_by_name(name) do
      :undefined ->
        :ok
      pid when is_pid(pid) ->
        Phoenix.Tracker.untrack(__MODULE__, pid, :swarm_names, name)
    end
  end

  @spec register_property(prop :: term, pid) :: :ok
  def register_property(prop, pid) do
    Phoenix.Tracker.track(__MODULE__, pid, prop, pid, %{node: node()})
  end

  @spec unregister_property(prop :: term, pid) :: :ok
  def unregister_property(prop, pid) do
    Phoenix.Tracker.untrack(__MODULE__, pid, prop, pid)
  end

  @spec get_by_name(name :: term) :: :undefined | pid
  def get_by_name(name) do
    case GenServer.call(__MODULE__, {:list, :swarm_names})
      |> Phoenix.Tracker.State.get_by_topic(:swarm_names)
      |> Enum.find(fn {{_topic, _pid, ^name}, _meta, _tag} -> true; _ -> false end) do
        {{_topic, pid, _name}, _meta, _tag} -> pid
        nil -> :undefined
    end
  end

  @spec get_by_property(prop :: term) :: [pid]
  def get_by_property(prop) do
    GenServer.call(__MODULE__, {:list, prop})
    |> Phoenix.Tracker.State.get_by_topic(prop)
    |> Enum.map(fn {{_topic, pid, _name}, _meta, _tag} -> pid end)
  end

  # Phoenix.Tracker implementation

  def start_link(opts \\ []) do
    pubsub_server = Keyword.fetch!(Application.get_env(:swarm, :pubsub), :name)
    full_opts = Keyword.merge([name: __MODULE__, pubsub_server: pubsub_server], opts)
    GenServer.start_link(Phoenix.Tracker, [__MODULE__, full_opts, full_opts], opts)
  end

  def init(opts) do
    server = Keyword.fetch!(opts, :pubsub_server)
    {:ok, %{pubsub_server: server}}
  end

  @doc false
  def redistribute! do
    current_node = Node.self
    GenServer.call(__MODULE__, {:list, :swarm_names})
    |> Phoenix.Tracker.State.get_by_topic(:swarm_names)
    |> Enum.each(fn {{_topic, pid, name}, meta, _tag} ->
      Task.Supervisor.async_nolink(Swarm.TaskSupervisor, fn ->
        try do
          case Swarm.Ring.node_for_key(name) do
            # this node and target node are the same
            ^current_node ->
              case meta do
                # pid node matches the target node, nothing to do
                %{node: ^current_node, mfa: _mfa} ->
                  :ok
                # pid node does not match the target node, restart the pid
                # on the correct node
                %{node: other_node, mfa: mfa} ->
                  # if we're no longer connected to the other node, restart the pid
                  unless Enum.member?(Node.list, other_node) do
                    debug "restarting #{inspect name} on #{current_node}: connection to previous node #{other_node} was lost"
                    restart_pid(pid, name, mfa)
                  end
                # this is a simple registration, do nothing
                _meta ->
                  :ok
              end
            # this node and the target node are not the same
            other_node ->
              case meta do
                # pid node does not match the target node, and the pid is local
                %{node: ^current_node, mfa: mfa} ->
                  debug "handoff: initiating for #{inspect name} on #{current_node}, new parent is #{other_node}"
                  # perform handoff
                  case GenServer.call(pid, {:swarm, :begin_handoff}, :infinity) do
                    :restart ->
                      debug "handoff: #{inspect name} requested restart"
                      restart_pid(pid, name, mfa)
                    {:resume, state} ->
                      debug "handoff: #{inspect name} requested to be resumed, restarting.."
                      {:ok, new_pid} = restart_pid(pid, name, mfa)
                      debug "handoff: #{inspect name} restart complete, resuming"
                      GenServer.cast(new_pid, {:swarm, :end_handoff, state})
                    :ignore ->
                      debug "handoff: #{inspect name} requested to be ignored"
                      :ok
                  end
                # pid node may or may not match the target node, but the pid is
                # non-local, so ignore it
                _ ->
                  :ok
              end
          end
        catch
          _type, err ->
            error "failed to redistribute #{inspect pid} (#{Process.alive?(pid)}) on #{Node.self}: #{inspect err}"
        end
      end)
    end)
  end

  # Responsible for resolving conflicts and handling registry changes
  @doc false
  def handle_diff(diff, state) do
    for {_type, {joins, leaves}} <- diff do
      # first remove all leaves
      for {name, %{pid: pid}} <- leaves do
        :ets.delete(:swarm_registry, {name, pid})
      end
      # then apply all joins
      for {name, %{pid: pid} = meta} <- joins do
        dups = :ets.match_object(:swarm_registry, {{name, :'$1'}, :'$2'})
        # kill duplicate pids which are managed by swarm, last write wins
        for {{_, duplicate_pid} = duplicate, %{mfa: _}} <- dups do
          unless duplicate_pid == pid do
            debug "duplicate registration for #{inspect name} (#{inspect duplicate_pid}), resolving.."
            send(duplicate_pid, {:swarm, :die})
          end
          :ets.delete(:swarm_registry, duplicate)
        end
        :ets.insert(:swarm_registry, {{name, pid}, meta})
      end
    end
    {:ok, state}
  end

  defp do_register(name, pid, meta) do
    meta = Enum.into(meta, %{node: node(), pid: pid})
    {:ok, _ref} = Phoenix.Tracker.track(__MODULE__, pid, :swarm_names, name, meta)
    {:ok, pid}
  end

  # Performs the steps necessary to properly restart a process
  defp restart_pid(pid, name, {mod, fun, args}) do
    if node(pid) == Node.self do
      send(pid, {:swarm, :die})
    end
    unregister_name(name)
    register_name(name, mod, fun, args, [checked: false])
  end
end
