# Swarm

[![Hex.pm Version](http://img.shields.io/hexpm/v/swarm.svg?style=flat)](https://hex.pm/packages/swarm)

Swarm is both a global distributed registry, like `gproc`, and a clustering utility.
It was designed for the use case where large numbers of persistent processes are created
for things like devices, and are unique across a cluster of Erlang nodes, and messages
must be routed to those processes, both individually, and in groups. Additionally, Swarm
is designed to distribute these processes evenly across the cluster based on a consistent
hashing algorithm, and automatically move processes in response to cluster topology changes,
or node crashes.

View the docs [here](https://hexdocs.pm/swarm).

## Installation

```elixir
defp deps do
  [{:swarm, "~> 0.2"}]
end
```

## Features

- automatic cluster formation/healing based on a gossip protocol
  via UDP, using a configurable port/multicast address, or alternatively
  if running under Kubernetes, via a configurable pod selector and node
  basename.
- automatic distribution of registered processes across
  the cluster based on a consistent hashing algorithm,
  where names are partitioned across nodes based on their hash.
- easy handoff of processes between one node and another, including
  handoff of current process state. You may indicate whether the
  handoff should simply restart the process on the new node, start
  the process and then send it the handoff message containing state,
  or ignore the handoff and remain on it's current node.
- can do simple registration with `{:via, :swarm, name}`
- both an Erlang and Elixir API

## Restrictions

- auto-balancing of processes in the cluster require registrations be done via
  `register_name/4`, which takes module/function/args params, and handles starting
  the process for you. The MFA must return `{:ok, pid}` or a plain pid.
  This is how Swarm handles process handoff between nodes, and automatic restarts when nodedown
  events occur and the cluster topology changes.


## Consistency Guarantees

Like any distributed system, a choice must be made in terms of guarantees provided. 
Swarm favors availability over consistency, even though it is eventually consistent, as network partitions,
when healed, will be resolved by asking any copies of a given name that live on nodes where they don't
belong, to shutdown, or kill them if they do not.

Network partitions result in all partitions running an instance of processes created with Swarm.
Swarm was designed for use in an IoT platform, where process names are generally based on physical device ids,
and as such, the consistency issue is less of a problem. If events get routed to two separate partitions,
it's generally not an issue if those events are for the same device. However this is clearly not ideal
in all situations. Swarm also aims to be fast, so registrations and lookups must be as low latency as possible,
even when the number of processes in the registry grows very large. This is acheived without consensus by using
a consistent hash of the name which deterministically defines which node a process belongs on, and all requests
to start a process on that node will be serialized through that node to prevent conflicts.

## Clustering

You have three choices with regards to cluster management. You can use the built-in Erlang tooling for connecting
nodes, by setting `autocluster: false` in the config for `swarm`. If set to `autocluster: true` it will make use of Swarm's 
dynamic cluster formation via multicast UDP. If set to `autocluster: :kubernetes`, it will use the Kubernetes API, and
the token/namespace injected into the pod to form a cluster of nodes based on a pod selector. You can provide your own
autoclustering implementation by setting `autocluster: MyApp.Module` where `MyApp.Module` is an OTP process
(i.e. `GenServer`, something started with `:proc_lib`, etc.). The implementation must connect nodes with `:net_adm.connect_node/1`.

The gossip protocol works by multicasting a heartbeat via UDP. The default configuration listens on all host interfaces,
port 45892, and publishes via the multicast address `230.1.1.251`. These parameters can all be changed via the
following config settings:

```elixir
config :swarm,
  autocluster: true,
  port: 45892,
  if_addr: {0,0,0,0},
  multicast_addr: {230,1,1,251},
  # a TTL of 1 remains on the local network,
  # use this to change the number of jumps the
  # multicast packets will make
  multicast_ttl: 1
```

The Kubernetes strategy works by querying the Kubernetes API for all endpoints in the same namespace which match the provided
selector, and getting the container IPs associated with them. Once all of the matching IPs have been found, it will attempt to 
establish node connections using the format `<kubernetes_node_basename>@<endpoint ip>`. You must make sure that your nodes are 
configured to use longnames, that the hostname matches the `kubernetes_node_basename` setting, and that the domain matches the 
IP address. Configuration might look like so:

```elixir
config :swarm,
  autocluster: :kubernetes,
  kubernetes_selector: "app=myapp",
  kubernetes_node_basename: "myapp"
```

And in vm.args:

```
-name myapp@10.128.0.9
-setcookie test
```

In all configurations, Swarm will respond to nodeup/nodedown events by shifting registered processes
around the cluster based on the hash of their name.

## Autojoin

Swarm will automatically join the cluster, however in some situations this can result in undesirable behaviour,
namely, since Swarm starts before your application is started, if your application takes longer than it takes Swarm
to start redistributing processes to the new node, and those processes need to be attached to a supervisor which is not
yet running, an error will occur.

To handle this, you can set `autojoin: false` in the config. You must then explicitly join the cluster with `Swarm.join!/0`.
Until you do this, Swarm will still connect nodes, but will treat the new node as if it didn't exist until `join!` is called.

## Registration/Process Grouping

Swarm is intended to be used by registering processes *before* they are created, and letting Swarm start
them for you on the proper node in the cluster. This is done via `register_name/4`. You may also register
processes the normal way, i.e. `GenServer.start_link({:via, :swarm, name}, ...)`. Swarm will manage these
registrations, and replicate them across the cluster, however these processes will not be moved in response
to cluster topology changes.

Swarm also offers process grouping, similar to the way `gproc` does properties. You "join" a process to a group
after it's started, typically in `init/1`, with `Swarm.join/2`. You can then publish messages (i.e. `cast`) with
`Swarm.publish/2`, and/or call all processes in a group and collect results (i.e. `call`) with `Swarm.multicall/2` or
`Swarm.multicall/3`. Leaving a group can be done with `Swarm.leave/2`, but will automatically be done when a process
dies. Join/leave can be used to do pubsub like things, or perform operations over a group of related processes.

## Example

The following example shows a simple case where workers are dynamically created in response
to some events under a supervisor, and we want them to be distributed across the cluster and
be discoverable by name from anywhere in the cluster. Swarm is a perfect fit for this
situation.

```elixir
defmodule MyApp.WorkerSup do
  @moduledoc """
  This is the supervisor for the worker processes you wish to distribute
  across the cluster, Swarm is primarily designed around the use case
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
  # in cluster topology, valid response values are:
  #
  #   - `:restart`, to simply restart the process on the new node
  #   - `{:resume, state}`, to hand off some state to the new process
  #   - `:ignore`, to leave the process running on it's current node
  #
  def handle_call({:swarm, :begin_handoff}, {name, delay}) do
    {:reply, {:resume, delay}, {name, delay}}
  end
  # called after the process has been restarted on it's new node,
  # and the old process's state is being handed off. This is only
  # sent if the return to `begin_handoff` was `{:resume, state}`.
  # **NOTE**: This is called *after* the process is successfully started,
  # so make sure to design your processes around this caveat if you
  # wish to hand off state like this.
  def handle_call({:swarm, :end_handoff, delay}, {name, _}) do
    {:reply, :ok, {name, delay}}
  end
  def handle_call(_, _, state), do: {:noreply, state}

  def handle_info(:timeout, {name, delay}) do
    IO.puts "#{inspect name} says hi!"
    Process.send_after(self(), :timeout, delay)
    {:noreply, {name, delay}}
  end
  # this message is sent when this process should die
  # because it's being moved, use this as an opportunity
  # to clean up
  def handle_info({:swarm, :die}, state) do
    {:stop, :shutdown, state}
  end
  def handle_info(_, state), do: {:noreply, state}
end

defmodule MyApp.ExampleUsages do
  ...snip...

  @doc """
  Starts worker and registers name in the cluster, then joins the process
  to the `:foo` group
  """
  def start_worker(name) do
    {:ok, pid} = Swarm.register(name, MyApp.Supervisor, :register, [name: name])
    Swarm.join(:foo, pid)
  end

  @doc """
  Gets the pid of the worker with the given name
  """
  def get_worker(name), do: Swarm.whereis_name(name)

  @doc """
  Gets all of the pids that are members of the `:foo` group
  """
  def get_foos(), do: Swarm.members(:foo)

  @doc """
  Call some worker by name
  """
  def call_worker(name, msg), do: GenServer.call({:via, :swarm, name}, msg)

  @doc """
  Cast to some worker by name
  """
  def cast_worker(name, msg), do: GenServer.cast({:via, :swarm, name}, msg)

  @doc """
  Publish a message to all members of group `:foo`
  """
  def publish_foos(msg), do: Swarm.publish(:foo, msg)

  @doc """
  Call all members of group `:foo` and collect the results,
  any failures or nil values are filtered out of the result list
  """
  def call_foos(msg), do: Swarm.multicall(:foo, msg)

  ...snip...
end
```

## License

MIT

## TODO

- automated testing
- QuickCheck model
