# Distable

This project is an attempt at a smart distributed registry, which automatically
shifts processes around based on changes to cluster topology. It is designed
for Elixir and Erlang apps built on OTP conventions.

## Installation

```elixir
defp deps do
  [{:distable, "~> 0.1.0"}]
end
```

## Features

- automatic cluster formation/healing based on gossip
  via UDP, using a configurable port/multicast address
- automatic distribution of registered processes across
  the cluster based on a consistent hashing algorithm,
  where names are partitioned across nodes based on their hash.
- easy handoff of processes between one node and another, including
  handoff of current process state. You may indicate whether the
  handoff should simply restart the process on the new node, start
  the process and then send it the handoff message containing state,
  or ignore the handoff and remain on it's current node.

## Restrictions

- currently cannot be used with the `via` tuple for name registration
  as part of `start_link`, this may change
- registrations with `distable` are done with name and module/function/args,
  where the MFA must return a `{:ok, pid}` tuple, or a plain pid. This is how
  `distable` handles process handoff between nodes, and automatic restarts when nodedown
  events occur and the cluster topology changes.

## Example

The following example shows a simple case where workers are dynamically created in response
to some events under a supervisor, and we want them to be distributed across the cluster and
be discoverable by name from anywhere in the cluster. Distable is a perfect fit for this
situation.

```elixir
defmodule MyApp.WorkerSup do
  @moduledoc """
  This is the supervisor for the worker processes you wish to distribute
  across the cluster, Distable is primarily designed around the use case
  where you are dynamically creating many workers in response to events. It
  works with other use cases as well, but that's the ideal use case.
  """
  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    children = [
      worker(MyApp.Worker, [], restart: :transient)
    ]
    supervise(children, strategy: :simple_one_for_one)
  end

  @doc """
  Registers a new worker, and creates the worker process
  """
  def register(worker_args) when is_list(worker_args) do
    {:ok, _pid} = Supervisor.start_child(__MODULE__, worker_args)
  end
end

defmodule MyApp.Worker do
  @moduledoc """
  This is the worker process, in this case, it simply posts on a
  random recurring interval to stdout.
  """
  def start_link([name: name]), do: GenServer.start_link(__MODULE__, [name])
  def init(name), do: {:ok, {name, :rand.uniform(5_000)}, 0}

  # called when a handoff has been initiated due to changes
  # in cluster topology
  def handle_call({:distable, :begin_handoff}, {name, delay}) do
    {:reply, {:resume, delay}, {name, delay}}
  end
  # called after the process has been restarted and state
  # is being handed off to the new process, this is only
  # sent if the return to `begin_handoff` was `{:resume, state}`.
  def handle_call({:distable, :end_handoff, delay}, {name, _}) do
    {:reply, :ok, {name, delay}}
  end
  def handle_call(_, _, state), do: {:noreply, state}

  def handle_info(:timeout, {name, delay}) do
    IO.puts "#{inspect name} says hi!"
    Process.send_after(self(), :timeout, delay)
    {:noreply, {name, delay}}
  end
  # this message is sent when this process should die
  # so that it may shutdown cleanly
  def handle_info({:distable, :die}, state) do
    {:stop, :shutdown, state}
  end
  def handle_info(_, state), do: {:noreply, state}
end

defmodule MyApp.Listener do
  ...snip...
  def start_worker(name) do
    Distable.register(name, MyApp.Supervisor, :register, [name: name])
  end
  ...snip...
end
```

## License

MIT

## TODO

- testing
- documentation
- code cleanup
