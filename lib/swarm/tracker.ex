defmodule Swarm.Tracker do
  import Swarm.Entry
  alias Swarm.IntervalTreeClock, as: ITC
  alias Swarm.{Registry, Ring}

  defmodule TrackerState do
    defstruct clock: nil,
              nodes: [],
              ring: nil
  end

  # Public API

  @doc """
  Tracks a process (pid) with the given name.
  Tracking processes with this function will *not* restart the process when
  it's parent node goes down, or shift the process to other nodes if the cluster
  topology changes. It is strictly for global name registration.
  """
  def track(name, pid), do: GenServer.call(__MODULE__, {:track, name, pid, nil}, :infinity)

  @doc """
  Tracks a process created via the provided module/function/args with the given name.
  The process will be distributed on the cluster based on the hash of it's name on the hash ring.
  If the process's parent node goes down, it will be restarted on the new node which own's it's keyspace.
  If the cluster topology changes, and the owner of it's keyspace changes, it will be shifted to
  the new owner, after initiating the handoff process as described in the documentation.
  """
  def track(name, m, f, a), do: GenServer.call(__MODULE__, {:track, name, m, f, a}, :infinity)

  @doc """
  Stops tracking the given process (pid)
  """
  def untrack(pid), do: GenServer.call(__MODULE__, {:untrack, pid}, :infinity)

  @doc """
  Adds some metadata to the given process (pid). This is primarily used for tracking group membership.
  """
  def add_meta(key, value, pid), do: GenServer.cast(__MODULE__, {:add_meta, key, value, pid})

  @doc """
  Removes metadata from the given process (pid).
  """
  def remove_meta(key, pid), do: GenServer.cast(__MODULE__, {:remove_meta, key, pid})

  ## Process Internals / Internal API

  defmacrop handle_debug(debug, msg) do
    {current_state, _arity} = __CALLER__.function
    quote do
      :sys.handle_debug(unquote(debug), &write_debug/3, unquote(current_state), unquote(msg))
    end
  end

  defmacrop log(msg) do
    {current_state, _arity} = __CALLER__.function
    quote do
      Swarm.Logger.debug("[tracker:#{unquote(current_state)}] #{unquote(msg)}")
    end
  end
  defmacrop warn(msg) do
    {current_state, _arity} = __CALLER__.function
    quote do
      Swarm.Logger.warn("[tracker:#{unquote(current_state)}] #{unquote(msg)}")
    end
  end

  def start_link() do
    :proc_lib.start_link(__MODULE__, :init, [self()], :infinity, [:link])
  end

  # When the tracker starts, it does the following:
  #    - Registers itself as Swarm.Tracker
  #    - Traps exits so we know when we need to shutdown, or when started
  #      processes exit
  #    - Monitors node up/down events so we can keep the hash ring up to date
  #    - Collects the initial nodelist, and builds the hash ring and tracker state
  #    - Tells the parent process that we've started
  def init(parent) do
    # register as Swarm.Tracker
    Process.register(self(), __MODULE__)
    # Trap exits
    Process.flag(:trap_exit, true)
    # Tell the supervisor we've started
    :proc_lib.init_ack(parent, {:ok, self()})
    # Start monitoring nodes
    :ok = :net_kernel.monitor_nodes(true, [])
    log "started"
    # Before we can be considered "up", we must sync with
    # some other node in the cluster, if they exist, otherwise
    # we seed our own ITC and start tracking
    debug = :sys.debug_options([])
    # wait for node list to populate
    ring = Enum.reduce(Node.list, Ring.new(Node.self), fn n, r ->
      Ring.add_node(r, n)
    end)
    state = %TrackerState{nodes: Node.list, ring: ring}
    try do
      # start internal state machine
      change_state(:cluster_wait, state, parent, debug)
    catch
      kind, err ->
        IO.puts Exception.format(kind, err, System.stacktrace)
        IO.puts "Swarm.Tracker terminating abnormally.."
        exit({kind, err})
    end
  end

  defp change_state(next_state, state, parent, debug, extra \\ nil) do
    case apply(__MODULE__, next_state, [state, parent, debug, extra]) do
      {new_next_state, new_state, _parent, new_debug} ->
        new_debug = handle_debug(new_debug, "state transition to #{new_next_state}")
        change_state(new_next_state, new_state, parent, new_debug, nil)
      {new_next_state, new_state, _parent, new_debug, new_extra} ->
        new_debug = handle_debug(new_debug, "state transition to #{new_next_state} with #{inspect new_extra}")
        change_state(new_next_state, new_state, parent, new_debug, new_extra)
      {:exit, reason} ->
        exit(reason)
    end
  end

  # This phase of the state machine is just to ensure we give enough time for
  # nodes in the cluster to find and connect to each other before we attempt to join it.
  @doc false
  def cluster_wait(%TrackerState{nodes: nodes, ring: ring} = state, parent, debug, extra) do
    receive do
      {:system, from, request} ->
        :sys.handle_system_msg(request, from, parent, __MODULE__, debug, {:cluster_wait, state})
      {:EXIT, ^parent, reason} ->
        log "exiting: #{inspect reason}"
        exit(reason)
      {:nodeup, node} ->
        debug = handle_debug(debug, {:in, {:nodeup, node}})
        cond do
          Enum.member?(nodes, node) ->
            cluster_wait(state, parent, debug, extra)
          :else ->
            cluster_wait(%TrackerState{nodes: [node|nodes], ring: Ring.add_node(ring, node)}, parent, debug, extra)
        end
      {:nodedown, node} ->
        debug = handle_debug(debug, {:in, {:nodedown, node}})
        cond do
          Enum.member?(nodes, node) ->
            cluster_wait(%TrackerState{nodes: nodes -- [node], ring: Ring.remove_node(ring, node)}, parent, debug, extra)
          :else ->
            cluster_wait(state, parent, debug, extra)
        end
    after
      1_000 ->
        {:cluster_join, state, parent, debug}
    end
  end

  # This is where we kick off joining and synchronizing with the cluster
  # If there are no known remote nodes, then we become the "seed" node of
  # the cluster, in the sense that our clock will be the initial version
  # of the clock which all other nodes will fork from
  @doc false
  def cluster_join(%TrackerState{nodes: []} = state, parent, debug, _extra) do
    log "joining cluster.."
    log "no connected nodes, proceeding without sync"
    :timer.send_after(5 * 60_000, self(), :anti_entropy)
    {:tracking, %{state | clock: ITC.seed()}, parent, debug}
  end
  def cluster_join(%TrackerState{nodes: nodelist} = state, parent, debug, _extra) do
    log "joining cluster.."
    log "found connected nodes: #{inspect nodelist}"
    # Connect to a random node and sync registries,
    # start anti-entropy, and start loop with forked clock of
    # remote node
    sync_node = Enum.random(nodelist)
    log "selected sync node: #{sync_node}"
    # Send sync request
    GenServer.cast({__MODULE__, sync_node}, {:sync, self()})
    debug = handle_debug(debug, {:out, {:sync, self()}, {__MODULE__, sync_node}})
    {:syncing, state, parent, debug, sync_node}
  end

  # This is the receive loop for the initial synchronization phase.
  # The primary goal of this phase is to get the tracker in sync with at
  # least one other node in the cluster, or determine if we should be the seed
  # node for the cluster
  #
  # Terminology
  # - "source node": the node initiating the sync request
  # - "target node": the current target of our sync request
  #
  # It is very complex for a number of reasons:
  #    - Nodes can be starting up simultaneously which presents the following problems:
  #      - Two nodes in the cluster can select each other, which in a naive implementation
  #        would deadlock the two processes while they wait for each other to respond.
  #      - While waiting for the target node to respond, we may receive requests from other
  #        nodes which want to sync with us, depending on the situation we may need to tell them
  #        to try a different node, or hold on to the request until we sync with our target, and
  #        then pass that data along to all pending requests.
  #      - This loop must implement both the source and target node state machines
  #    - Our target node may go down in the middle of synchronization, if that happens,
  #      we need to detect it, select a new target node if available, and either start the sync
  #      process again, or become the seed node ourselves.
  #
  # At some point I'll put together an ASCII art diagram of the FSM
  @doc false
  def syncing(state, parent, debug, sync_node) when is_atom(sync_node),
    do: syncing(state, parent, debug, {sync_node, []})
  def syncing(%TrackerState{nodes: nodes, ring: ring} = state, parent, debug, {sync_node, pending_requests}) do
    receive do
      {:system, from, request} ->
        :sys.handle_system_msg(request, from, parent, __MODULE__, debug, {:syncing, state, {sync_node, pending_requests}})
      {:EXIT, ^parent, reason} ->
        log "exiting: #{inspect reason}"
        exit(reason)
      # We want to handle nodeup/down events while syncing so that if we need to select
      # a new node, we can do so.
      {:nodeup, node} ->
        log "node up #{node}"
        debug = handle_debug(debug, {:in, {:nodeup, node}})
        cond do
          Enum.member?(nodes, node) ->
            syncing(state, parent, debug, {sync_node, pending_requests})
          :else ->
            new_state = %{state | nodes: [node|nodes], ring: Ring.add_node(ring, node)}
            syncing(new_state, parent, debug, {sync_node, pending_requests})
        end
      # If our target node goes down, we need to select a new target or become the seed node
      {:nodedown, ^sync_node} ->
        log "the selected sync node #{sync_node} went down, selecting new node"
        debug = handle_debug(debug, {:in, {:nodedown, sync_node}})
        case nodes -- [sync_node] do
          [] ->
            # there are no other nodes to select, we'll be the seed
            log "no other available nodes, becoming seed node"
            %{state | nodes: [], ring: Ring.remove_node(ring, sync_node), clock: ITC.seed()}
          nodes ->
            new_sync_node = Enum.random(nodes)
            log "selected sync node: #{new_sync_node}"
            # Send sync request
            GenServer.cast({__MODULE__, new_sync_node}, {:sync, self()})
            debug = handle_debug(debug, {:out, {:sync, self()}, {__MODULE__, new_sync_node}})
            new_state = %{state | nodes: nodes, ring: Ring.remove_node(ring, sync_node)}
            syncing(new_state, parent, debug, {new_sync_node, pending_requests})
        end
      # Keep the hash ring up to date if other nodes go down
      {:nodedown, node} ->
        debug = handle_debug(debug, {:in, {:nodedown, node}})
        cond do
          Enum.member?(nodes, node) ->
            log "node down #{node}"
            new_state = %{state | nodes: nodes -- [node], ring: Ring.remove_node(ring, node)}
            syncing(new_state, parent, debug, {sync_node, pending_requests})
          :else ->
            syncing(state, parent, debug, {sync_node, pending_requests})
        end
      # Receive a copy of the registry from our sync target
      {:sync_recv, from, clock, registry} ->
        debug = handle_debug(debug, {:in, {:sync_recv, from, clock, :swarm_registry}})
        log "received sync response, loading registry.."
        # let remote node know we've got the registry
        send(from, {:sync_ack, Node.self})
        debug = handle_debug(debug, {:out, {:sync_ack, Node.self}, from})
        # load target registry
        for entry(name: name, pid: pid, meta: meta, clock: clock) <- registry do
          ref = Process.monitor(pid)
          :ets.insert(:swarm_registry, entry(name: name, pid: pid, ref: ref, meta: meta, clock: clock))
        end
        # update our local clock to match remote clock
        state = %{state | clock: clock}
        log "finished sync and sent acknowledgement to #{node(from)}"
        # clear any pending sync requests to prevent blocking
        {:resolve_pending_sync_requests, state, parent, debug, pending_requests}
      # Something weird happened during sync, so try a different node,
      # with this implementation, we *could* end up selecting the same node
      # again, but that's fine as this is effectively a retry
      {:sync_err, from} when length(nodes) > 0 ->
        debug = handle_debug(debug, {:in, {:sync_err, from}, from})
        log "sync error, choosing a new node to sync with"
        # we need to choose a different node to sync with and try again
        new_sync_node = Enum.random(nodes)
        GenServer.cast({__MODULE__, sync_node}, {:sync, self()})
        debug = handle_debug(debug, {:out, {:sync, self()}, {__MODULE__, sync_node}})
        syncing(state, parent, debug, {new_sync_node, pending_requests})
      # Something went wrong during sync, but there are no other nodes to sync with,
      # not even the original sync node (which probably implies it shutdown or crashed),
      # so we're the sync node now
      {:sync_err, from} ->
        debug = handle_debug(debug, {:in, {:sync_err, from}, from})
        log "sync error, and no other available sync targets, becoming seed node"
        {:resolve_pending_sync_requests, %{state | clock: ITC.seed()}, parent, debug, pending_requests}
      # Incoming sync request from our target node, in other words, A is trying to sync with B,
      # and B is trying to sync with A - we need to break the deadlock
      {:'$gen_cast', {:sync, from}} = msg when node(from) == sync_node ->
        debug = handle_debug(debug, {:in, msg, from})
        log "received sync request during initial sync"
        cond do
          # Well shit, we can't pass the request off to another node, so let's roll a die
          # to choose who will sync with who.
          length(nodes) == 1 ->
            # 20d, I mean why not?
            die = :rand.uniform(20)
            log "there is a tie between syncing nodes, breaking with die roll (#{die}).."
            msg = {:sync_break_tie, self(), die}
            send(from, {:sync_break_tie, self(), die})
            debug = handle_debug(debug, {:out, msg, from})
            {:sync_tiebreaker, state, parent, debug, {sync_node, die, pending_requests}}
          :else ->
            log "rejecting sync request since we're still in initial sync"
            # there are other nodes, tell the requesting node to choose another node to sync with
            # and then we'll sync with them
            send(from, {:sync_err, self()})
            debug = handle_debug(debug, {:out, {:sync_err, self()}, from})
            syncing(state, parent, debug, {sync_node, pending_requests})
        end
      {:'$gen_cast', {:sync, from}} = msg ->
        debug = handle_debug(debug, {:in, msg, from})
        log "pending sync request from #{node(from)}"
        syncing(state, parent, debug, {sync_node, Enum.uniq([from|pending_requests])})
    after 30_000 ->
        log "failed to sync with #{sync_node} after 30s, selecting a new node and retrying"
        case nodes -- [sync_node] do
          [] ->
            log "no other nodes to sync with, becoming seed node"
            new_state = %{state | clock: ITC.seed(), nodes: [], ring: Ring.remove_node(ring, sync_node)}
            {:resolve_pending_sync_requests, new_state, parent, debug, pending_requests}
          nodes ->
            new_sync_node = Enum.random(nodes)
            log "selected new sync node #{new_sync_node}"
            GenServer.cast({__MODULE__, new_sync_node}, {:sync, self()})
            debug = handle_debug(debug, {:out, {:sync, self()}, {__MODULE__, new_sync_node}})
            new_state = %{state | nodes: nodes, ring: Ring.remove_node(ring, sync_node)}
            syncing(state, parent, debug, {new_sync_node, pending_requests})
        end
    end
  end

  # This state occurs as a sub-state of `syncing`, when two nodes attempt to sync with
  # each other, and there is no alternative but to choose which one is the "dominant" node
  # for the sync operation
  @doc false
  def sync_tiebreaker(%TrackerState{nodes: nodes} = state, parent, debug, {sync_node, die, pending_requests}) do
    receive do
      {:system, from, request} ->
        extra = {:sync_tiebreaker, state, {sync_node, die, pending_requests}}
        :sys.handle_system_msg(request, from, parent, __MODULE__, debug, extra)
      {:EXIT, ^parent, reason} ->
        log "exiting: #{inspect reason}"
        exit(reason)
      {:nodedown, ^sync_node} = msg ->
        debug = handle_debug(debug, {:in, msg})
        # welp, guess we'll try a new node
        case nodes -- [sync_node] do
          [] ->
            log "sync with #{sync_node} cancelled: nodedown, no other nodes available, so becoming seed"
            new_state = %{state | nodes: [], ring: Ring.remove_node(state.ring, sync_node)}
            {:resolve_pending_sync_requests, new_state, parent, debug, pending_requests}
          nodes ->
            log "sync with #{sync_node} cancelled: nodedown, retrying with a new node"
            sync_node = Enum.random(nodes)
            GenServer.cast({__MODULE__, sync_node}, {:sync, self()})
            debug = handle_debug(debug, {:out, {:sync, self()}, {__MODULE__, sync_node}})
            new_state = %{state | nodes: nodes, ring: Ring.remove_node(state.ring, sync_node)}
            {:syncing, new_state, parent, debug, {sync_node, pending_requests}}
        end
      {:sync_break_tie, from, die2} = msg when die2 > die or (die2 == die and node(from) > node(self)) ->
        debug = handle_debug(debug, {:in, msg, from})
        log "#{node(from)} won the die roll (#{die2} vs #{die}), waiting for payload.."
        # The other node won the die roll, either by a greater die roll, or the absolute
        # tie breaker of node ordering
        {:syncing, state, parent, debug, {sync_node, pending_requests}}
      {:sync_break_tie, from, die2} = msg ->
        debug = handle_debug(debug, {:in, msg, from})
        log "we won the die roll (#{die} vs #{die2}), sending payload.."
        # This is the new seed node
        {clock, rclock} = ITC.fork(ITC.seed())
        send(from, {:sync_recv, self(), rclock, []})
        debug = handle_debug(debug, {:out, {:sync_recv, self(), rclock, []}, from})
        extra = {{:sync_ack, sync_node}, :resolve_pending_sync_requests, pending_requests}
        {:waiting, %{state | clock: clock}, parent, debug, extra}
    end
  end

  # This state occurs after `syncing` and before `tracking`,
  # and cleans up pending sync requests from remote nodes
  @doc false
  def resolve_pending_sync_requests(state, parent, debug, []) do
    log "pending sync requests cleared"
    {:tracking, state, parent, debug}
  end
  def resolve_pending_sync_requests(%TrackerState{} = state, parent, debug, [pid|pending]) do
    pending_node = node(pid)
    cond do
      Enum.member?(state.nodes, pending_node) ->
        log "clearing pending sync request for #{pending_node}"
        {lclock, rclock} = ITC.fork(state.clock)
        send(pid, {:sync_recv, self(), rclock, :ets.tab2list(:swarm_registry)})
        debug = handle_debug(debug, {:out, {:sync_recv, self(), rclock, :swarm_registry}, pid})
        extra = {{:sync_ack, node(pid)}, :resolve_pending_sync_requests, pending}
        {:waiting, %{state | clock: lclock}, parent, debug, extra}
      :else ->
        {:tracking, state, parent, debug}
    end
  end


  # This state waits for a specific reply from a given remote_node, and
  # changes state to the provided `next_state` when complete.
  # If the remote node goes down, waiting is cancelled
  @doc false
  def waiting(state, parent, debug, {{reply, remote_node}, next_state, next_state_extra}) do
    receive do
      {:system, from, request} ->
        sys_state = {:waiting, state, {{reply, remote_node}, next_state, next_state_extra}}
        :sys.handle_system_msg(request, from, parent, __MODULE__, debug, sys_state)
      {:EXIT, ^parent, reason} ->
        log "exiting: #{inspect reason}"
        exit(reason)
      {^reply, ^remote_node} = msg ->
        debug = handle_debug(debug, {:in, msg})
        log "wait for #{inspect reply} from #{remote_node} complete"
        {next_state, state, parent, debug, next_state_extra}
      {:nodedown, ^remote_node} = msg ->
        debug = handle_debug(debug, {:in, msg})
        warn "wait for #{remote_node} cancelled, node went down"
        {next_state, state, parent, debug, next_state_extra}
      {:nodedown, node} = msg ->
        debug = handle_debug(debug, {:in, msg})
        cond do
          Enum.member?(state.nodes, node) ->
            new_state = %{state | nodes: [node|state.nodes], ring: Ring.add_node(state.ring, node)}
            waiting(new_state, parent, debug, {{reply, remote_node}, next_state, next_state_extra})
          :else ->
            waiting(state, parent, debug, {{reply, remote_node}, next_state, next_state_extra})
        end
      {:nodeup, node} = msg ->
        debug = handle_debug(debug, {:in, msg})
        cond do
          Enum.member?(state.nodes, node) ->
            waiting(state, parent, debug, {{reply, remote_node}, next_state, next_state_extra})
          :else ->
            new_state = %{state | nodes: [node|state.nodes], ring: Ring.add_node(state.ring, node)}
            waiting(new_state, parent, debug, {{reply, remote_node}, next_state, next_state_extra})
        end
    after 1_000 ->
        log "waiting for #{inspect reply} from #{remote_node}.."
        waiting(state, parent, debug, {{reply, remote_node}, next_state, next_state_extra})
    end
  end

  # This is the primary state/receive loop. It's complexity is in large part due to the fact that we
  # need to prioritize certain messages as they have implications on how things are tracked, as
  # well as the need to ensure that one or more instances of the tracker can't deadlock each other
  # when they need to coordinate. The implementation of these is almost entirely implemented via
  # handle_* functions to keep the loop clearly structured.
  @doc false
  def tracking(state, parent, debug, extra) do
    receive do
      # System messages take precedence, as does the parent process exiting
      {:system, from, request} ->
        :sys.handle_system_msg(request, from, parent, __MODULE__, debug, {:tracking, state, extra})
      # Parent supervisor is telling us to exit
      {:EXIT, ^parent, reason} ->
        log "exiting: #{inspect reason}"
        exit(reason)
      # Task started by the tracker exited
      {:EXIT, _child, _reason} ->
        tracking(state, parent, debug, extra)
      # Cluster topology change events
      {:nodeup, node} = msg ->
        debug = handle_debug(debug, {:in, msg})
        {:nodeup, state, parent, debug, {node, :tracking, extra}}
      {:nodedown, node} = msg ->
        debug = handle_debug(debug, {:in, msg})
        {:nodedown, state, parent, debug, {node, :tracking, extra}}
      :anti_entropy ->
        debug = handle_debug(debug, {:in, :anti_entropy})
        {:anti_entropy, state, parent, debug, extra}
      # Received a handoff request from a node
      {from, {:handoff, name, m, f, a, handoff_state, rclock}} = msg ->
        debug = handle_debug(debug, {:in, msg, from})
        {:handoff, state, parent, debug, {name, m, f, a, handoff_state, rclock}}
      # A remote registration failed due to nodedown during the call
      {:retry, {:track, name, m, f, a, from}} = msg ->
        debug = handle_debug(debug, {:in, msg, from})
        {:retry_track, state, parent, debug, {name, m, f, a, from}}
      # A change event received from another node
      {:event, from, rclock, event} = msg ->
        debug = handle_debug(debug, {:in, msg, from})
        {:handle_event, state, parent, debug, {event, from, rclock}}
      # A change event received locally
      {:'$gen_call', from, call} = msg ->
        debug = handle_debug(debug, {:in, msg, from})
        {:handle_call, state, parent, debug, {from, call}}
      {:'$gen_cast', cast} = msg ->
        debug = handle_debug(debug, {:in, msg})
        {:handle_cast, state, parent, debug, cast}
      {:DOWN, ref, _type, pid, info} = msg ->
        debug = handle_debug(debug, {:in, msg})
        {:handle_monitor, state, parent, debug, {ref, pid, info}}
      msg ->
        debug = handle_debug(debug, {:in, msg})
        log "unexpected message: #{inspect msg}"
        tracking(state, parent, debug, extra)
    end
  end

  # This state helps us ensure that nodes proactively keep themselves synced
  # after joining the cluster and initial syncrhonization. This way if replication
  # events fail for some reason, we can control the drift in registry state
  @doc false
  def anti_entropy(%TrackerState{nodes: []} = state, parent, debug, nil) do
    :timer.send_after(5 * 60_000, self(), :anti_entropy)
    {:tracking, state, parent, debug}
  end
  def anti_entropy(%TrackerState{nodes: nodes} = state, parent, debug, nil) do
    sync_node = Enum.random(nodes)
    log "syncing with #{sync_node}"
    GenServer.cast({__MODULE__, sync_node}, {:sync, self()})
    debug = handle_debug(debug, {:out, {:sync, self()}, {__MODULE__, sync_node}})
    {:anti_entropy, state, parent, debug, {sync_node, :erlang.timestamp()}}
  end
  def anti_entropy(%TrackerState{nodes: nodes} = state, parent, debug, {sync_node, start_time}) do
    receive do
      {:system, from, request} ->
        :sys.handle_system_msg(request, from, parent, __MODULE__, debug, {:anti_entropy, state, {sync_node, start_time}})
      {:EXIT, ^parent, reason} ->
        log "exiting: #{inspect reason}"
        exit(reason)
      {:nodedown, ^sync_node} = msg ->
        debug = handle_debug(debug, {:in, msg})
        warn "sync with #{sync_node} failed: nodedown, aborting this pass"
        state
      {:nodedown, node} = msg ->
        debug = handle_debug(debug, {:in, msg})
        cond do
          Enum.member?(state.nodes, node) ->
            new_state = %{state | nodes: nodes -- [node], ring: Ring.remove_node(state.ring, node)}
            anti_entropy(new_state, parent, debug, {sync_node, start_time})
          :else ->
            anti_entropy(state, parent, debug, {sync_node, start_time})
        end
      {:nodeup, node} = msg ->
        debug = handle_debug(debug, {:in, msg})
        cond do
          Enum.member?(state.nodes, node) ->
            anti_entropy(state, parent, debug, {sync_node, start_time})
          :else ->
            new_state = %{state | nodes: [node|state.nodes], ring: Ring.add_node(state.ring, node)}
            anti_entropy(new_state, parent, debug, {sync_node, start_time})
        end
      {:sync, node} = msg ->
        debug = handle_debug(debug, {:in, msg})
        log "received sync request from #{node}, sending registry.."
        send({__MODULE__, sync_node}, {:sync_recv, self(), ITC.peek(state.clock), :ets.tab2list(:swarm_registry)})
        debug = handle_debug(debug, {:out, {:sync_recv, self(), ITC.peek(state.clock), :swarm_registry}})
        anti_entropy(state, parent, debug, {sync_node, start_time})
      {:sync_recv, from, _rclock, registry} = msg ->
        debug = handle_debug(debug, {:in, msg, from})
        log "received registry from #{sync_node}, checking.."
        # let remote node know we've got the registry
        send(from, {:sync_ack, Node.self})
        debug = handle_debug(debug, {:out, {:sync_ack, Node.self}, from})
        # map over the registry and check that all local entries are correct
        new_state = Enum.reduce(registry, state, fn
          entry(name: rname, pid: rpid, meta: rmeta, clock: rclock) = rreg, %TrackerState{clock: clock} = state ->
          case :ets.lookup(:swarm_registry, rname) do
            [] ->
              # missing local registration
              cond do
                ITC.leq(rclock, clock) ->
                  # our clock is ahead, so we'll do nothing
                  log "local is missing #{inspect rname}, but local clock is dominant, ignoring"
                  state
                :else ->
                  # our clock is behind, so add the registration
                  log "local is missing #{inspect rname}, adding.."
                  ref = Process.monitor(rpid)
                  :ets.insert(:swarm_registry, entry(name: rname, pid: rpid, ref: ref, meta: rmeta, clock: rclock))
                  %{state | clock: ITC.event(clock)}
              end
            [entry(pid: ^rpid, meta: ^rmeta, clock: ^rclock)] ->
              # this entry matches, nothing to do
              state
            [entry(pid: ^rpid, meta: ^rmeta, clock: _)] ->
              # the clocks differ, but the data is identical, so let's update it so they're the same
              :ets.update_element(:swarm_registry, rname, [{entry(:clock)+1, rclock}])
            [entry(pid: ^rpid, meta: lmeta, clock: lclock)] ->
              # the metadata differs, we need to merge it
              cond do
                ITC.leq(lclock, rclock) ->
                  # the remote clock dominates, so merge favoring data from the remote registry
                  new_meta = Map.merge(lmeta, rmeta)
                  :ets.update_element(:swarm_registry, rname, [{entry(:meta)+1, new_meta}])
                  %{state | clock: ITC.event(clock)}
                ITC.leq(rclock, lclock) ->
                  # the local clock dominates, so merge favoring data from the local registry
                  new_meta = Map.merge(rmeta, lmeta)
                  :ets.update_element(:swarm_registry, rname, [{entry(:meta)+1, new_meta}])
                  %{state | clock: ITC.event(clock)}
                :else ->
                  # the clocks conflict, but there's nothing we can do, so warn and then keep the local
                  # view for the time being
                  warn "local and remote metadata for #{inspect rname} conflicts, keeping local for now"
                  state
              end
            [entry(pid: lpid, clock: lclock) = lreg] ->
              # there are two different processes for the same name, we need to resolve
              cond do
                ITC.leq(lclock, rclock) ->
                  # the remote registration is newer
                  resolve_incorrect_local_reg(sync_node, lreg, rreg, state)
                ITC.leq(rclock, lclock) ->
                  # the local registration is newer
                  log "remote view of #{inspect rname} is outdated, resolving.."
                  resolve_incorrect_remote_reg(sync_node, lreg, rreg, state)
                :else ->
                  # the clocks conflict, determine which registration is correct based on current topology
                  # and resolve the conflict
                  rpid_node    = node(rpid)
                  lpid_node    = node(lpid)
                  current_node = Node.self
                  target_node  = Ring.key_to_node(state.ring, rname)
                  cond do
                    target_node == rpid_node and lpid_node != rpid_node ->
                      log "remote and local view of #{inspect rname} conflict, but remote is correct, resolving.."
                      resolve_incorrect_local_reg(sync_node, lreg, rreg, state)
                    target_node == lpid_node and rpid_node != lpid_node ->
                      log "remote and local view of #{inspect rname} conflict, but local is correct, resolving.."
                      resolve_incorrect_remote_reg(sync_node, lreg, rreg, state)
                    lpid_node == rpid_node && rpid > lpid ->
                      log "remote and local view of #{inspect rname} conflict, " <>
                        "but the remote view is more recent, resolving.."
                      resolve_incorrect_local_reg(sync_node, lreg, rreg, state)
                    lpid_node == rpid_node && lpid > rpid ->
                      log "remote and local view of #{inspect rname} conflict, " <>
                        "but the local view is more recent, resolving.."
                      resolve_incorrect_remote_reg(sync_node, lreg, rreg, state)
                    :else ->
                      # the registrations are inconsistent with each other, and cannot be resolved
                      warn "remote and local view of #{inspect rname} conflict, " <>
                        "but cannot be resolved"
                      state
                  end
              end
          end
        end)
        log "completed in #{Float.round(:timer.now_diff(:erlang.timestamp(), start_time)/1_000)}"
        {:tracking, new_state, parent, debug}
    end
  end

  # Used during anti-entropy checks to remove local registrations and replace them with the remote version
  defp resolve_incorrect_local_reg(_remote_node, entry(pid: lpid) = lreg, entry(name: rname, pid: rpid, meta: rmeta, clock: rclock), state) do
    # the remote registration is correct
    {:noreply, state} = remove_registration(state, lreg)
    send(lpid, {:swarm, :die})
    # add the remote registration
    ref = Process.monitor(rpid)
    clock = ITC.event(state.clock)
    :ets.insert(:swarm_registry, entry(name: rname, pid: rpid, ref: ref, meta: rmeta, clock: rclock))
    %{state | clock: clock}
  end

  # Used during anti-entropy checks to remove remote registrations and replace them with the local version
  defp resolve_incorrect_remote_reg(remote_node, entry(pid: lpid, meta: lmeta), entry(name: rname, pid: rpid), state) do
    send({__MODULE__, remote_node}, {:untrack, rpid})
    send(rpid, {:swarm, :die})
    send({__MODULE__, remote_node}, {:track, rname, lpid, lmeta})
    state
  end

  # A new node has been added to the cluster, we need to update the hash ring and handle shifting
  # processes to new nodes based on the new topology.
  @doc false
  def nodeup(%TrackerState{nodes: nodes, ring: ring} = state, parent, debug, {node, next_state, next_state_extra}) do
    cond do
      Enum.member?(nodes, node) ->
        {next_state, state, parent, debug, next_state_extra}
      :else ->
        case :rpc.call(node, :application, :ensure_all_started, [:swarm]) do
          {:ok, _} ->
            log "nodeup #{node}"
            new_state = %{state | nodes: [node|nodes], ring: Ring.add_node(ring, node)}
            {:topology_change, new_state, parent, debug, {:nodeup, node, next_state, next_state_extra}}
          _ ->
            {next_state, state, parent, debug, next_state_extra}
        end
    end
  end

  # A remote node went down, we need to update the hash ring and handle restarting/shifting processes
  # as needed based on the new topology
  @doc false
  def nodedown(%TrackerState{nodes: nodes, ring: ring} = state, parent, debug, {node, next_state, next_state_extra}) do
    cond do
      Enum.member?(nodes, node) ->
        log "nodedown #{node}"
        ring = Ring.remove_node(ring, node)
        new_state = %{state | nodes: nodes -- [node], ring: Ring.remove_node(ring, node)}
        {:topology_change, new_state, parent, debug, {:nodedown, node, next_state, next_state_extra}}
      :else ->
        {next_state, state, parent, debug, next_state_extra}
    end
  end
  # This is the callback for when a process is being handed off from a remote node to this node.
  @doc false
  def handoff(%TrackerState{clock: clock} = state, parent, debug, {_from, name, m, f, a, handoff_state, _rclock}) do
    try do
      {:ok, pid} = apply(m, f, a)
      GenServer.cast(pid, {:swarm, :end_handoff, handoff_state})
      debug = handle_debug(debug, {:out, {:swarm, :end_handoff, handoff_state}, pid})
      ref = Process.monitor(pid)
      clock = ITC.event(clock)
      meta = %{mfa: {m,f,a}}
      :ets.insert(:swarm_registry, entry(name: name, pid: pid, ref: ref, meta: meta, clock: ITC.peek(clock)))
      broadcast_event(state.nodes, ITC.peek(clock), {:track, name, pid, meta})
      {:tracking, %{state | clock: clock}, parent, debug}
    catch
      kind, err ->
        IO.puts Exception.normalize(kind, err, System.stacktrace)
        {:tracking, state, parent, debug}
    end
  end

  # This is the callback for when a nodeup/down event occurs after the tracker has entered
  # the main receive loop. Topology changes are handled a bit differently during startup.
  @doc false
  def topology_change(%TrackerState{} = state, parent, debug, {type, remote_node, next_state, next_extra}) do
    log "topology change (#{type} for #{remote_node})"
    current_node = Node.self
    clock = :ets.foldl(fn
      entry(name: name, pid: pid, meta: %{mfa: {m,f,a}}) = obj, lclock when node(pid) == current_node ->
        case Ring.key_to_node(state.ring, name) do
          ^current_node ->
            # This process is correct
            lclock
          other_node ->
            log "#{inspect pid} belongs on #{other_node}"
            # This process needs to be moved to the new node
            try do
              debug = handle_debug(debug, {:out, {:swarm, :begin_handoff}, pid})
              case GenServer.call(pid, {:swarm, :begin_handoff}) do
                :ignore ->
                  log "#{inspect pid} has requested to be ignored"
                  lclock
                {:resume, handoff_state} ->
                  log "#{inspect pid} has requested to be resumed"
                  {:noreply, state} = remove_registration(%{state | clock: lclock}, obj)
                  send(pid, {:swarm, :die})
                  debug = handle_debug(debug, {:out, {:swarm, :die}, pid})
                  log "sending handoff to #{remote_node}"
                  out_msg = {self(), {:handoff, name, m, f, a, handoff_state, ITC.peek(state.clock)}}
                  send({__MODULE__, remote_node}, out_msg)
                  debug = handle_debug(debug, {:out, out_msg, {__MODULE__, remote_node}})
                  state.clock
                :restart ->
                  log "#{inspect pid} has requested to be restarted"
                  {:noreply, state} = remove_registration(%{state | clock: lclock}, obj)
                  send(pid, {:swarm, :die})
                  debug = handle_debug(debug, {:out, {:swarm, :die}, pid})
                  {:reply, _, state} = handle_call({:track, name, m, f, a}, nil, %{state | clock: lclock})
                  state.clock
              end
            catch
              _, err ->
                warn "handoff failed for #{inspect pid}: #{inspect err}"
                lclock
            end
        end
      entry(name: name, pid: pid), lclock ->
        log "doing nothing for #{inspect name}, it is owned by #{node(pid)}"
        lclock
    end, state.clock, :swarm_registry)
    log "topology change complete"
    {next_state, %{state | clock: clock}, parent, debug, next_extra}
  end

  # This is the callback for tracker events which are being replicated from other nodes in the cluster
  @doc false
  def handle_event(%TrackerState{clock: clock} = state, parent, debug, {{:track, name, pid, meta}, _from, rclock}) do
    log "event: track #{inspect {name, pid, meta}}"
    cond do
      ITC.leq(clock, rclock) ->
        ref = Process.monitor(pid)
        :ets.insert(:swarm_registry, entry(name: name, pid: pid, ref: ref, meta: meta, clock: rclock))
        {:tracking, %{state | clock: ITC.event(clock)}, parent, debug}
      :else ->
        warn "received track event for #{inspect name}, but local clock conflicts with remote clock, event unhandled"
        # TODO: Handle conflict?
        {:tracking, state, parent, debug}
    end
  end
  def handle_event(%TrackerState{clock: clock} = state, parent, debug, {{:untrack, pid}, _from, rclock}) do
    log "event: untrack #{inspect pid}"
    cond do
      ITC.leq(clock, rclock) ->
        case Registry.get_by_pid(pid) do
          :undefined ->
            {:tracking, state, parent, debug}
          entry(ref: ref, clock: lclock) = obj ->
            cond do
              ITC.leq(lclock, rclock) ->
                # registration came before unregister, so remove the registration
                Process.demonitor(ref, [:flush])
                :ets.delete_object(:swarm_registry, obj)
                {:tracking, %{state | clock: ITC.event(clock)}, parent, debug}
              :else ->
                # registration is newer than de-registration, ignore msg
                log "untrack is causally dominated by track for #{inspect pid}, ignoring.."
                {:tracking, state, parent, debug}
            end
        end
      :else ->
        warn "received untrack event, but local clock conflicts with remote clock, event unhandled"
        # Handle conflict?
        {:tracking, state, parent, debug}
    end
  end
  def handle_event(%TrackerState{clock: clock} = state, parent, debug, {{:add_meta, key, value, pid}, _from, rclock}) do
    log "event: add_meta #{inspect {key, value}} to #{inspect pid}"
    case Registry.get_by_pid(pid) do
      :undefined ->
        {:tracking, state, parent, debug}
      entry(name: name, meta: old_meta, clock: lclock) ->
        cond do
          ITC.leq(lclock, rclock) ->
            new_meta = Map.put(old_meta, key, value)
            :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, rclock}])
            {:tracking, %{state | clock: ITC.event(clock)}, parent, debug}
          :else ->
            # we're going to take the last-writer wins approach for resolution for now
            new_meta = Map.merge(old_meta, %{key => value})
            # we're going to keep our local clock though and re-broadcast the update to ensure we converge
            clock = ITC.event(clock)
            :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, ITC.peek(clock)}])
            log "conflicting meta for #{inspect name}, updating and notifying other nodes"
            broadcast_event(state.nodes, ITC.peek(clock), {:update_meta, new_meta, pid})
            {:tracking, %{state | clock: clock}, parent, debug}
        end
    end
  end
  def handle_event(%TrackerState{clock: clock} = state, parent, debug, {{:update_meta, new_meta, pid}, _from, rclock}) do
    log "event: update_meta #{inspect new_meta} for #{inspect pid}"
    case Registry.get_by_pid(pid) do
      :undefined ->
        {:tracking, state, parent, debug}
      entry(name: name, meta: old_meta, clock: lclock) ->
        cond do
          ITC.leq(lclock, rclock) ->
            :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, rclock}])
            {:tracking, %{state | clock: ITC.event(clock)}, parent, debug}
          :else ->
            # we're going to take the last-writer wins approach for resolution for now
            new_meta = Map.merge(old_meta, new_meta)
            # we're going to keep our local clock though and re-broadcast the update to ensure we converge
            clock = ITC.event(clock)
            :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, ITC.peek(clock)}])
            log "conflicting meta for #{inspect name}, updating and notifying other nodes"
            broadcast_event(state.nodes, ITC.peek(clock), {:update_meta, new_meta, pid})
            {:tracking, %{state | clock: clock}, parent, debug}
        end
    end
  end
  def handle_event(%TrackerState{clock: clock} = state, parent, debug, {{:remove_meta, key, pid}, _from, rclock}) do
    log "event: remove_meta #{inspect key} from #{inspect pid}"
    case Registry.get_by_pid(pid) do
      :undefined ->
        {:tracking, state, parent, debug}
      entry(name: name, meta: meta, clock: lclock) ->
        cond do
          ITC.leq(lclock, rclock) ->
            new_meta = Map.drop(meta, [key])
            :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, rclock}])
            {:tracking, %{state | clock: ITC.event(clock)}, parent, debug}
          :else ->
            warn "received remove_meta event, but local clock conflicts with remote clock, event unhandled"
            # Handle conflict?
            {:tracking, state, parent, debug}
        end
    end
  end

  # This is the handler for local operations on the tracker which require a response.
  @doc false
  def handle_call(%TrackerState{} = state, parent, debug, {from, call}) do
    case handle_call(call, from, state) do
      {:reply, reply, state} ->
        GenServer.reply(from, reply)
        {:tracking, state, parent, debug}
      {:noreply, state} ->
        {:tracking, state, parent, debug}
    end
  end
  defp handle_call({:track, name, pid, meta}, _from, %TrackerState{} = state) do
    log "call: track #{inspect {name, pid, meta}}"
    add_registration(state, name, pid, meta)
  end
  defp handle_call({:track, name, m, f, a}, from, %TrackerState{ring: ring} = state) do
    log "call: track #{inspect {name, m, f, a}}"
    current_node = Node.self
    case Ring.key_to_node(ring, name) do
      ^current_node ->
        log "starting #{inspect {m,f,a}} on #{current_node}"
        case Registry.get_by_name(name) do
          :undefined ->
            try do
              case apply(m, f, a) do
                {:ok, pid} ->
                  add_registration(state, name, pid, %{mfa: {m,f,a}})
                err ->
                  {:reply, {:error, {:invalid_return, err}}, state}
              end
            catch
              _, reason ->
                {:reply, {:error, reason}, state}
            end
          entry(pid: pid) ->
            {:reply, {:error, {:already_registered, pid}}, state}
        end
      remote_node ->
        log "starting #{inspect {m,f,a}} on #{remote_node}"
        Task.start(fn ->
          try do
            reply = GenServer.call({__MODULE__, remote_node}, {:track, name, m, f, a}, :infinity)
            GenServer.reply(from, reply)
          catch
            _, err ->
              warn "failed to start #{inspect name} on #{remote_node}: #{inspect err}, retrying operation.."
              send(__MODULE__, {:retry, {:track, name, m, f, a, from}})
          end
        end)
        {:noreply, state}
    end
  end

  # This is the handler for local operations on the tracker which are asynchronous
  @doc false
  def handle_cast(%TrackerState{} = state, parent, debug, {:untrack, pid}) do
    log "call: untrack #{inspect pid}"
    {:noreply, state} = remove_registration_by_pid(state, pid)
    {:tracking, state, parent, debug}
  end
  def handle_cast(%TrackerState{} = state, parent, debug, {:add_meta, key, value, pid}) do
    log "call: add_meta #{inspect {key, value}} to #{inspect pid}"
    {:noreply, state} = add_meta_by_pid(state, {key, value}, pid)
    {:tracking, state, parent, debug}
  end
  def handle_cast(%TrackerState{} = state, parent, debug, {:remove_meta, key, pid}) do
    log "call: remote_meta #{inspect key} for #{inspect pid}"
    {:noreply, state} = remove_meta_by_pid(state, key, pid)
    {:tracking, state, parent, debug}
  end
  def handle_cast(%TrackerState{clock: clock} = state, parent, debug, {:sync, from}) do
    log "received sync request from #{node(from)}"
    {lclock, rclock} = ITC.fork(clock)
    send(from, {:sync_recv, self(), rclock, :ets.tab2list(:swarm_registry)})
    debug = handle_debug(debug, {:out, {:sync_recv, self(), rclock, :swarm_registry}, from})
    {:waiting, %{state | clock: lclock}, parent, debug, {{:sync_ack, node(from)}, :tracking, nil}}
  end

  # This is only ever called if a registration needs to be sent to a remote node
  # and that node went down in the middle of the call to it's Swarm process.
  # We need to process the nodeup/down events by re-entering the receive loop first,
  # so we send ourselves a message to retry, this is the handler for that message
  @doc false
  def retry_track(state, parent, debug, {name, m, f, a, from}) do
    {:handle_call, state, parent, debug, {{:track, name, m, f, a}, from}}
  end

  # Called when a pid dies, and the monitor is triggered
  @doc false
  def handle_monitor(%TrackerState{ring: ring} = state, parent, debug, {ref, pid, :noconnection}) do
    # lost connection to the node this pid is running on, check if we should restart it
    log "lost connection to pid (#{inspect pid}), checking to see if we should revive it"
    case Registry.get_by_ref(ref) do
      :undefined ->
        log "could not find pid #{inspect pid}"
        {:tracking, state, parent, debug}
      entry(name: name, pid: ^pid, ref: ^ref, meta: %{mfa: {m,f,a}}) = obj ->
        current_node = Node.self
        # this event may have occurred before we get a nodedown event, so
        # for the purposes of this handler, preemptively remove the node from the
        # ring when calculating the new node
        state = %{state | ring: Ring.remove_node(state.ring, node(pid))}
        case Ring.key_to_node(ring, name) do
          ^current_node ->
            log "restarting pid #{inspect pid} on #{current_node}"
            {:noreply, state}  = remove_registration(state, obj)
            {:reply, _, state} = handle_call({:track, name, m, f, a}, nil, state)
            {:tracking, state, parent, debug}
          other_node ->
            log "pid belongs on #{other_node}"
            {:noreply, state} = remove_registration(state, obj)
            {:tracking, state, parent, debug}
        end
      entry(pid: pid, ref: ^ref, meta: meta) = obj ->
        log "nothing to do for pid #{inspect pid} (meta: #{inspect meta})"
        {:noreply, state} = remove_registration(state, obj)
        {:tracking, state, parent, debug}
    end
  end
  def handle_monitor(%TrackerState{} = state, parent, debug, {ref, pid, reason}) do
    log "pid (#{inspect pid}) down: #{inspect reason}"
    case Registry.get_by_ref(ref) do
      :undefined ->
        {:tracking, state, parent, debug}
      entry(ref: ^ref) = obj ->
        {:noreply, state} = remove_registration(state, obj)
        {:tracking, state, parent, debug}
    end
  end

  # Sys module callbacks

  # Handle resuming this process after it's suspended by :sys
  # We're making a bit of an assumption here that it won't be suspended
  # prior to entering the receive loop. This is something we (probably) could
  # fix by storing the current phase of the startup the process is in, but I'm not sure.
  def system_continue(parent, debug, {next_state, state}),
    do: system_continue(parent, debug, {next_state, state, nil})
  def system_continue(parent, debug, {next_state, state, extra}),
    do: apply(__MODULE__, next_state, [state, parent, debug, extra])

  # Handle system shutdown gracefully
  def system_terminate(_reason, :application_controller, _debug, _state) do
    # OTP-5811 Don't send an error report if it's the system process
    # application_controller which is terminating - let init take care
    # of it instead
    :ok
  end
  def system_terminate(:normal, _parent, _debug, _state) do
    exit(:normal)
  end
  def system_terminate(reason, _parent, debug, state) do
    :error_logger.format('** ~p terminating~n
                          ** Server state was: ~p~n
                          ** Reason: ~n** ~p~n', [__MODULE__, state, reason])
    :sys.print_log(debug)
    exit(reason)
  end

  # Used for fetching the current process state
  def system_get_state({_next_state, state}) do
    {:ok, state}
  end

  # Called when someone asks to replace the current process state
  # Required, but you really really shouldn't do this.
  def system_replace_state(state_fun, {next_state, state}) do
    new_state = state_fun.(state)
    {:ok, {next_state, new_state}, {next_state, new_state}}
  end

  # Called when the system is upgrading this process
  def system_code_change(misc, _module, _old, _extra) do
    {:ok, misc}
  end

  # Used for tracing with sys:handle_debug
  defp write_debug(_dev, {:in, msg, from}, next_state) do
    Swarm.Logger.debug("[tracker:#{next_state}] <== #{inspect msg} from #{inspect from}")
  end
  defp write_debug(_dev, {:in, msg}, next_state) do
    Swarm.Logger.debug("[tracker:#{next_state}] <== #{inspect msg}")
  end
  defp write_debug(_dev, {:out, msg, to}, next_state) do
    Swarm.Logger.debug("[tracker:#{next_state}] ==> #{inspect msg} to #{inspect to}")
  end
  defp write_debug(_dev, {:out, msg}, next_state) do
    Swarm.Logger.debug("[tracker:#{next_state}] ==> #{inspect msg}")
  end
  defp write_debug(_dev, event, next_state) do
    Swarm.Logger.debug("[tracker:#{next_state}] #{inspect event}")
  end

  ## Internal helpers

  defp broadcast_event([], _clock, _event),  do: :ok
  defp broadcast_event(nodes, clock, event), do: :rpc.sbcast(nodes, __MODULE__, {:event, self(), clock, event})

  defp add_registration(%TrackerState{clock: clock, nodes: nodes} = state, name, pid, meta) do
    case Registry.get_by_name(name) do
      :undefined ->
        ref = Process.monitor(pid)
        clock = ITC.event(clock)
        :ets.insert(:swarm_registry, entry(name: name, pid: pid, ref: ref, meta: meta, clock: ITC.peek(clock)))
        broadcast_event(nodes, ITC.peek(clock), {:track, name, pid, meta})
        {:reply, {:ok, pid}, %{state | clock: clock}}
      entry(pid: pid) ->
        {:reply, {:error, {:already_registered, pid}}, state}
    end
  end

  defp remove_registration(%TrackerState{clock: clock} = state, entry(pid: pid, ref: ref) = obj) do
    Process.demonitor(ref, [:flush])
    :ets.delete_object(:swarm_registry, obj)
    clock = ITC.event(clock)
    broadcast_event(state.nodes, ITC.peek(clock), {:untrack, pid})
    {:noreply, %{state | clock: clock}}
  end


  defp remove_registration_by_pid(%TrackerState{clock: clock} = state, pid) do
    case Registry.get_by_pid(pid) do
      :undefined ->
        broadcast_event(state.nodes, ITC.peek(clock), {:untrack, pid})
        {:noreply, state}
      entry(ref: ref) = obj ->
        Process.demonitor(ref, [:flush])
        :ets.delete_object(:swarm_registry, obj)
        clock = ITC.event(clock)
        broadcast_event(state.nodes, ITC.peek(clock), {:untrack, pid})
        {:noreply, %{state | clock: clock}}
    end
  end

  defp add_meta_by_pid(%TrackerState{clock: clock} = state, {key, value}, pid) do
    case Registry.get_by_pid(pid) do
      :undefined ->
        broadcast_event(state.nodes, ITC.peek(clock), {:add_meta, key, value, pid})
        {:noreply, state}
      entry(name: name, meta: old_meta) ->
        new_meta = Map.put(old_meta, key, value)
        clock = ITC.event(clock)
        :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, ITC.peek(clock)}])
        broadcast_event(state.nodes, ITC.peek(clock), {:update_meta, new_meta, pid})
        {:noreply, %{state | clock: clock}}
    end
  end

  defp remove_meta_by_pid(%TrackerState{clock: clock} = state, key, pid) do
    case Registry.get_by_pid(pid) do
      :undefined ->
        broadcast_event(state.nodes, ITC.peek(clock), {:remove_meta, key, pid})
      entry(name: name, meta: old_meta) ->
        new_meta = Map.drop(old_meta, [key])
        :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, ITC.peek(clock)}])
        clock = ITC.event(clock)
        broadcast_event(state.nodes, ITC.peek(clock), {:update_meta, new_meta, pid})
        {:noreply, %{state | clock: clock}}
    end
  end
end
