# Swarm

[![Hex.pm Version](http://img.shields.io/hexpm/v/swarm.svg?style=flat)](https://hex.pm/packages/swarm)

**NOTE**: If you are upgrading from 1.0, be aware that the autoclustering functionality has been extracted
to its own package, which you will need to depend on if you use that feature.
The package is [libcluster](http://github.com/bitwalker/libcluster) and is available on 
[Hex](https://hex.pm/packages/libcluster). Please be sure to read over the README to make sure your 
config is properly updated.

Swarm is a global distributed registry, offering a feature set similar to that of `gproc`,
but architected to handle dynamic node membership and large volumes of process registrations
being created/removed in short time windows.

To be more clear, Swarm was born out of the need for a global process registry which could
handle large numbers of persistent processes representing devices/device connections, which
needed to be distributed around a cluster of Erlang nodes, and easily found. Messages need
to be routed to those processes from anywhere in the cluster, both individually, and as groups.
Additionally, those processes need to be shifted around the cluster based on cluster topology
changes, or restarted if their owning node goes down.

Before writing Swarm, I tried both `global` and `gproc`, but the former is not very flexible, and
both of them require leader election, which, in the face of dynamic node membership and the sheer
volume of registrations, ended up causing deadlocks/timeouts during leadership contention.

I also attempted to use `syn`, but because it uses `mnesia`, dynamic node membership as a requirement
means it's dead on arrival for my use case.

In short, are you running a cluster of Erlang nodes under something like Kubernetes? If so, Swarm is
for you!

View the docs [here](https://hexdocs.pm/swarm).

**PLEASE READ**: If you are giving Swarm a spin, it is important to understand that you can concoct scenarios whereby
the registry appears to be out of sync temporarily, this is a side effect of an eventually consistent model and does not mean that
Swarm is not working correctly, rather you need to ensure that applications you build on top of Swarm are written to embrace eventual
consistency, such that periods of inconsistency are tolerated. For the most part though, the registry replicates extremely
quickly, so noticeable inconsistency is more of an exception than a rule, but a proper distributed system should always be designed to
tolerate the exceptions, as they become more and more common as you scale up. If however you notice extreme inconsistency or delayed
replication, then it is possible it may be a bug, or performance issue, so feel free to open an issue if you are unsure, and we will gladly look into it.

## Installation

```elixir
defp deps do
  [{:swarm, "~> 3.0"}]
end
```

## Features

- automatic distribution of registered processes across
  the cluster based on a consistent hashing algorithm,
  where names are partitioned across nodes based on their hash.
- easy [handoff of processes](#process-handoff) between one node and another, including
  handoff of current process state.
- can do simple registration with `{:via, :swarm, name}`
- both an Erlang and Elixir API

## Restrictions

- auto-balancing of processes in the cluster requires registrations to be done via
  `register_name/5`, which takes module/function/args params, and handles starting
  the process for you. The MFA must return `{:ok, pid}`.
  This is how Swarm handles process handoff between nodes, and automatic restarts when nodedown
  events occur and the cluster topology changes.

### Process handoff

Processes may be redistributed between nodes when a node joins, or leaves, a cluster. You can indicate whether the handoff should simply restart the process on the new node, start the process and then send it the handoff message containing state, or ignore the handoff and remain on its current node.

Process state can be transferred between running nodes during process redistribution by using the `{:swarm, :begin_handoff}` and `{:swarm, :end_handoff, state}` callbacks. However process state will be lost when a node hosting a distributed process terminates. In this scenario you must restore the state yourself.

## Consistency Guarantees

Like any distributed system, a choice must be made in terms of guarantees provided. You can choose between
availability or consistency during a network partition by selecting the appropriate process distribution strategy.

Swarm provides two strategies for you to use:

- #### `Swarm.Distribution.Ring`

  This strategy favors availability over consistency, even though it is eventually consistent, as
  network partitions, when healed, will be resolved by asking any copies of a given name that live on
  nodes where they don't belong to shutdown.

  Network partitions result in all partitions running an instance of processes created with Swarm.
  Swarm was designed for use in an IoT platform, where process names are generally based on physical
  device ids, and as such, the consistency issue is less of a problem. If events get routed to two
  separate partitions, it's generally not an issue if those events are for the same device. However
  this is clearly not ideal in all situations. Swarm also aims to be fast, so registrations and
  lookups must be as low latency as possible, even when the number of processes in the registry grows
  very large. This is acheived without consensus by using a consistent hash of the name which
  deterministically defines which node a process belongs on, and all requests to start a process on
  that node will be serialized through that node to prevent conflicts.

  This is the default strategy and requires no configuration.

- #### `Swarm.Distribution.StaticQuorumRing`

  A quorum is the minimum number of nodes that a distributed cluster has to obtain in order to be
  allowed to perform an operation. This can be used to enforce consistent operation in a distributed
  system.

  You configure the quorum size by defining the minimum number of nodes that must be connected in the
  cluster to allow process registration and distribution. Calls to `Swarm.register_name/5` will return `{:error, :no_node_available}` if there are fewer nodes available than the configured minimum quorum size.

  In a network partition, the partition containing at least the quorum size number of clusters will
  continue operation. Processes running on the other side of the split will be stopped and restarted
  on the active side. This ensures that only one instance of a registered process will be running in
  the cluster.

  You must configure this strategy and its minimum quorum size using the `:static_quorum_size` setting:

  ```elixir
  config :swarm,
    distribution_strategy: Swarm.Distribution.StaticQuorumRing,
    static_quorum_size: 5
  ```

  The quorum size should be set to half the cluster size, plus one node. So a three node cluster
  would be two, a five node cluster is three, and a nine node cluster is five. You *must* not add more
  than 2 x quorum size - 1 nodes to the cluster as this would cause a network split to result in
  both partitions continuing operation.

  Processes are distributed amongst the cluster using the same consistent hash of their name as in
  the ring strategy above.

  This strategy is a good choice when you have a fixed number of nodes in the cluster.

## Clustering

Swarm pre-2.0 included auto-clustering functionality, but that has been split out into its own package,
[libcluster](https://github.com/bitwalker/libcluster). Swarm works out of the box with Erlang's distribution
tools (i.e. `Node.connect/1`, `:net_kernel.connect_node/1`, etc.), but if you need the auto-clustering that Swarm
previously provided, you will need to add `:libcluster` to your deps, and make sure it's in your applications
list *before* `:swarm`. Some of the configuration has changed slightly in `:libcluster`, so be sure to review
the docs.

### Node Blacklisting/Whitelisting

You can explicitly whitelist or blacklist nodes to prevent certain nodes from being included in Swarm's consistent
hash ring. This is done with either the `node_whitelist` and `node_blacklist` settings respectively. These settings
must be lists containing either literal strings or valid Elixir regex patterns as either string or regex literals.
If no whitelist is set, then the blacklist is used, and if no blacklist is provided, the default blacklist includes
two patterns, in both cases to ignore nodes which are created by Relx/ExRM/Distillery when using releases, in order
to setup remote shells (the first) and hot upgrade scripting (the second), the patterns can be found in this repo's
`config/config.exs` file, and you can find a quick example below:

```elixir
config :swarm,
  node_whitelist: [~r/^myapp-[\d]@.*$/]
```

The above will only allow nodes named something like `myapp-1@somehost` to be included in Swarm's clustering. **NOTE**:
It is important to understand that this does not prevent those nodes from connecting to the cluster, only that Swarm will
not include those nodes in its distribution algorithm, or communicate with those nodes.

## Registration/Process Grouping

Swarm is intended to be used by registering processes *before* they are created, and letting Swarm start
them for you on the proper node in the cluster. This is done via `Swarm.register_name/5`. You may also register
processes the normal way, i.e. `GenServer.start_link({:via, :swarm, name}, ...)`. Swarm will manage these
registrations, and replicate them across the cluster, however these processes will not be moved in response
to cluster topology changes.

Swarm also offers process grouping, similar to the way `gproc` does properties. You "join" a process to a group
after it is started, (beware of doing so in `init/1` outside of a Task, or it will deadlock), with `Swarm.join/2`.
You can then publish messages (i.e. `cast`) with
`Swarm.publish/2`, and/or call all processes in a group and collect results (i.e. `call`) with `Swarm.multi_call/2` or
`Swarm.multi_call/3`. Leaving a group can be done with `Swarm.leave/2`, but will automatically be done when a process
dies. Join/leave can be used to do pubsub like things, or perform operations over a group of related processes.

## Debugging/Troubleshooting

By configuring Swarm with `debug: true` and setting Logger's log level to `:debug`, you can get much more
information about what it is doing during operation to troubleshoot issues.

To dump the tracker's state, you can use `:sys.get_state(Swarm.Tracker)` or `:sys.get_status(Swarm.Tracker)`.
The former will dump the tracker state including what nodes it is tracking, what nodes are in the hash ring,
and the state of the interval tree clock. The latter will dump more detailed process info, including the current
function and its arguments. This is particularly useful if it appears that the tracker is stuck and not doing
anything. If you do find such things, please gist all of these results and open an issue so that I can fix these
issues if they arise.

## Example

The following example shows a simple case where workers are dynamically created in response
to some events under a supervisor, and we want them to be distributed across the cluster and
be discoverable by name from anywhere in the cluster. Swarm is a perfect fit for this
situation.

```elixir
defmodule MyApp.Supervisor do
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
      worker(MyApp.Worker, [], restart: :temporary)
    ]
    supervise(children, strategy: :simple_one_for_one)
  end

  @doc """
  Registers a new worker, and creates the worker process
  """
  def register(worker_name) do
    {:ok, _pid} = Supervisor.start_child(__MODULE__, [worker_name])
  end
end

defmodule MyApp.Worker do
  @moduledoc """
  This is the worker process, in this case, it simply posts on a
  random recurring interval to stdout.
  """
  def start_link(name) do
    GenServer.start_link(__MODULE__, [name])
  end

  def init([name]) do
    {:ok, {name, :rand.uniform(5_000)}, 0}
  end

  # called when a handoff has been initiated due to changes
  # in cluster topology, valid response values are:
  #
  #   - `:restart`, to simply restart the process on the new node
  #   - `{:resume, state}`, to hand off some state to the new process
  #   - `:ignore`, to leave the process running on its current node
  #
  def handle_call({:swarm, :begin_handoff}, _from, {name, delay}) do
    {:reply, {:resume, delay}, {name, delay}}
  end
  # called after the process has been restarted on its new node,
  # and the old process' state is being handed off. This is only
  # sent if the return to `begin_handoff` was `{:resume, state}`.
  # **NOTE**: This is called *after* the process is successfully started,
  # so make sure to design your processes around this caveat if you
  # wish to hand off state like this.
  def handle_cast({:swarm, :end_handoff, delay}, {name, _}) do
    {:noreply, {name, delay}}
  end
  # called when a network split is healed and the local process
  # should continue running, but a duplicate process on the other
  # side of the split is handing off its state to us. You can choose
  # to ignore the handoff state, or apply your own conflict resolution
  # strategy
  def handle_cast({:swarm, :resolve_conflict, _delay}, state) do
    {:noreply, state}
  end

  def handle_info(:timeout, {name, delay}) do
    IO.puts "#{inspect name} says hi!"
    Process.send_after(self(), :timeout, delay)
    {:noreply, {name, delay}}
  end
  # this message is sent when this process should die
  # because it is being moved, use this as an opportunity
  # to clean up
  def handle_info({:swarm, :die}, state) do
    {:stop, :shutdown, state}
  end
end

defmodule MyApp.ExampleUsage do
  ...snip...

  @doc """
  Starts worker and registers name in the cluster, then joins the process
  to the `:foo` group
  """
  def start_worker(name) do
    {:ok, pid} = Swarm.register_name(name, MyApp.Supervisor, :register, [name])
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
  def call_foos(msg), do: Swarm.multi_call(:foo, msg)

  ...snip...
end
```

## License

MIT

## TODO

- automated testing (some are present)
- QuickCheck model
