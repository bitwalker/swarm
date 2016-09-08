defmodule Swarm.Registry do
  @moduledoc """
  This process is the backing store for the registry. It should
  not be accessed directly, except by the Tracker process.

  It starts and owns an ETS table where the registry information is
  stored. It also exposes the registry via an API consumed by the Tracker.
  """
  #import Swarm.Logger
  require Logger

  @behaviour Phoenix.Tracker

  def start_link(opts \\ []) do
    pubsub_server = Application.get_env(:swarm, :pubsub)
                    |> Keyword.get(:name, Swarm.PubSub)
    full_opts = Keyword.merge([name: __MODULE__, pubsub_server: pubsub_server], opts)
    GenServer.start_link(Phoenix.Tracker, [__MODULE__, full_opts, full_opts], opts)
  end

  def init(opts) do
    server = Keyword.fetch!(opts, :pubsub_server)
    {:ok, %{pubsub_server: server}}
  end

  def register_name(name, pid, meta \\ []) do
    case get_by_name(name) do
      :undefined ->
        meta = Enum.into(meta, %{node: node()})
        {:ok, _ref} = Phoenix.Tracker.track(__MODULE__, pid, :swarm_names, name, meta)
        {:ok, pid}
      pid when is_pid(pid) ->
        {:error, {:already_registered, pid}}
    end
  end

  def register_name(name, module, fun, args) do
    try do
      node = Node.self
      case Swarm.Ring.node_for_key(name) do
        ^node ->
          case apply(module, fun, args) do
            {:ok, pid} -> register_name(name, pid, mfa: {module, fun, args})
            other -> other
          end
        other_node ->
          :rpc.call(other_node, __MODULE__, :register_name, [name, module, fun, args])
      end
    catch
      {_, err} ->
        {:error, err}
    end
  end

  def unregister_name(name) do
    case get_by_name(name) do
      :undefined -> :ok
      pid when is_pid(pid) ->
        Phoenix.Tracker.untrack(__MODULE__, pid, :swarm_names, name)
    end
  end

  def register_property(prop, pid) do
    Phoenix.Tracker.track(__MODULE__, pid, prop, pid, %{node: node()})
  end

  def unregister_property(prop, pid) do
    Phoenix.Tracker.untrack(__MODULE__, pid, prop, pid)
  end

  def get_by_name(name) do
    case GenServer.call(__MODULE__, {:list, :swarm_names})
      |> Phoenix.Tracker.State.get_by_topic(:swarm_names)
      |> Enum.find(fn {{_topic, _pid, ^name}, _meta, _tag} -> true; _ -> false end) do
      {{_topic, pid, _name}, _meta, _tag} -> pid
      nil -> :undefined
    end
  end

  def get_by_property(prop) do
    GenServer.call(__MODULE__, {:list, prop})
    |> Phoenix.Tracker.State.get_by_topic(prop)
    |> Enum.map(fn {{_topic, pid, _name}, _meta, _tag} -> pid end)
  end

  @doc false
  def redistribute! do
    current_node = Node.self
    GenServer.call(__MODULE__, {:list, :swarm_names})
    |> Phoenix.Tracker.State.get_by_topic(:swarm_names)
    |> Enum.each(fn {{_topic, pid, name}, meta, _tag} ->
      Task.Supervisor.async_nolink(Swarm.TaskSupervisor, fn ->
        try do
          case meta do
            %{node: ^current_node, mfa: mfa} ->
              case Swarm.Ring.node_for_key(name) do
                ^current_node ->
                  :ok
                _other_node ->
                  case GenServer.call(pid, {:swarm, :begin_handoff}, :infinity) do
                    :restart ->
                      restart_pid(pid, name, mfa)
                    {:resume, state} ->
                      {:ok, new_pid} = restart_pid(pid, name, mfa)
                      GenServer.cast(new_pid, {:swarm, :end_handoff, state})
                    :ignore ->
                      :ok
                  end
              end
            _ ->
              :ok
          end
        catch
          _type, err ->
            Logger.error "failed to redistribute #{inspect pid}: #{inspect err}"
        end
      end)
    end)
  end

  defp restart_pid(pid, name, {mod, fun, args}) do
    send(pid, {:swarm, :die})
    unregister_name(name)
    register_name(name, mod, fun, args)
  end

  @doc false
  def handle_diff(diff, state) do
    for {type, {joins, leaves}} <- diff do
      for {pid, meta} <- leaves do
        Phoenix.PubSub.direct_broadcast(node(), state.pubsub_server, type, {:leave, pid, meta})
      end
      for {pid, meta} <- joins do
        Phoenix.PubSub.direct_broadcast(node(), state.pubsub_server, type, {:join, pid, meta})
      end
    end
    {:ok, state}
  end

end
