defmodule Swarm.Monitor do
  use GenServer

  def watch(name, pid), do: GenServer.call(__MODULE__, {:monitor, pid, name})
  def unwatch(name),    do: GenServer.call(__MODULE__, {:demonitor, name})

  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)
  def init(_) do
    {:ok, []}
  end

  def handle_call({:monitor, pid, name}, _from, state) do
    ref = Process.monitor(pid)
    {:reply, :ok, [{pid, ref, name}|state]}
  end
  def handle_call({:demonitor, name}, _from, state) do
    refs = Enum.filter_map(state, fn {_pid, _ref, ^name} -> true; _ -> false end, fn {_, ref, _} -> ref end)
    for ref <- refs do
      true = Process.demonitor(ref)
    end
    new_state = Enum.filter(state, fn {_,_,^name} -> false; _ -> true end)
    {:reply, :ok, new_state}
  end

  def handle_info({:DOWN, ref, _type, pid, _info}, state) do
    case Enum.find(state, fn {_,^ref,_} -> true; _ -> false end) do
      {_,_,name} ->
        :ok = Phoenix.Tracker.untrack(Swarm.Registry, pid, :swarm_names, name)
      _ ->
        :ok
    end
    new_state = Enum.filter(state, fn {_,^ref,_} -> false; _ -> true end)
    {:noreply, new_state}
  end

  def terminate(_reason, state) do
    for {_pid, ref, _} <- state do
      true = Process.demonitor(ref)
    end
  end
end
