defmodule Distable.Supervisor do
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    children = [
      worker(Distable.ETS, []),
      worker(Distable.Tracker, []),
      worker(Distable.Heartbeat, [])
    ]
    supervise(children, strategy: :one_for_one)
  end

  def handle_call({:change_ets_owner, table, pid}, state) do
    true = :ets.give_away(table, pid, "")
    {:reply, :ok, state}
  end
  def handle_call(_, _from, state) do
    {:noreply, state}
  end
end
