defmodule Distable do
  use Application

  def start(_type, _args) do
    Distable.Supervisor.start_link()
  end

  def register(name, module, function, args)
    when is_atom(module) and is_atom(function) and is_list(args) do
    Distable.Tracker.register(name, {module, function, args})
  end
  def unregister(name) do
    Distable.Tracker.unregister(name)
  end
  def register_property(pid, prop) do
    Distable.Tracker.register_property(pid, prop)
  end
  def whereis(name) do
    Distable.Tracker.whereis(name)
  end
  def get_by_property(prop) do
    Distable.Tracker.get_by_property(prop)
  end
  def call(name, message, timeout \\ 5_000) do
    Distable.Tracker.call(name, message, timeout)
  end
  def cast(name, message) do
    Distable.Tracker.cast(name, message)
  end
end
