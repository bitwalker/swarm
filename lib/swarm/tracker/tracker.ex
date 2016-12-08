defmodule Swarm.Tracker do
  @moduledoc """
  This module implements the distributed tracker for process registrations and groups.
  It is implemented as a finite state machine, via `:gen_statem`.

  Each node Swarm runs on will have a single instance of this process, and the trackers will
  replicate data between each other, and/or forward requests to remote trackers as necessary.
  """
  use GenStateMachine, callback_mode: :state_functions

  @sync_nodes_timeout 5_000
  @default_anti_entropy_interval 5 * 60_000

  import Swarm.Entry
  alias Swarm.IntervalTreeClock, as: Clock
  alias Swarm.Registry

  defmodule TrackerState do
    @type t :: %__MODULE__{
      clock: nil | Swarm.IntervalTreeClock.t,
      ring: HashRing.t,
      self: atom(),
      sync_node: nil | atom(),
      sync_ref: nil | reference(),
      pending_sync_reqs: [pid()]
    }
    defstruct clock: nil,
              nodes: [],
              ring: nil,
              self: :'nonode@nohost',
              sync_node: nil,
              sync_ref: nil,
              pending_sync_reqs: []
  end

  # Public API

  @doc """
  Authoritatively looks up the pid associated with a given name.
  """
  def whereis(name),
    do: GenStateMachine.call(__MODULE__, {:whereis, name}, :infinity)

  @doc """
  Tracks a process (pid) with the given name.
  Tracking processes with this function will *not* restart the process when
  it's parent node goes down, or shift the process to other nodes if the cluster
  topology changes. It is strictly for global name registration.
  """
  def track(name, pid) when is_pid(pid),
    do: GenStateMachine.call(__MODULE__, {:track, name, pid, %{}}, :infinity)

  @doc """
  Tracks a process created via the provided module/function/args with the given name.
  The process will be distributed on the cluster based on the hash of it's name on the hash ring.
  If the process's parent node goes down, it will be restarted on the new node which own's it's keyspace.
  If the cluster topology changes, and the owner of it's keyspace changes, it will be shifted to
  the new owner, after initiating the handoff process as described in the documentation.
  """
  def track(name, m, f, a) when is_atom(m) and is_atom(f) and is_list(a),
    do: GenStateMachine.call(__MODULE__, {:track, name, m, f, a}, :infinity)

  @doc """
  Stops tracking the given process (pid)
  """
  def untrack(pid) when is_pid(pid),
    do: GenStateMachine.call(__MODULE__, {:untrack, pid}, :infinity)

  @doc """
  Adds some metadata to the given process (pid). This is primarily used for tracking group membership.
  """
  def add_meta(key, value, pid) when is_pid(pid),
    do: GenStateMachine.call(__MODULE__, {:add_meta, key, value, pid}, :infinity)

  @doc """
  Removes metadata from the given process (pid).
  """
  def remove_meta(key, pid) when is_pid(pid),
    do: GenStateMachine.call(__MODULE__, {:remove_meta, key, pid}, :infinity)

  ## Process Internals / Internal API

  defmacrop debug(msg) do
    {current_state, _arity} = __CALLER__.function
    quote do
      Swarm.Logger.debug("[tracker:#{unquote(current_state)}] #{unquote(msg)}")
    end
  end
  defmacrop info(msg) do
    {current_state, _arity} = __CALLER__.function
    quote do
      Swarm.Logger.info("[tracker:#{unquote(current_state)}] #{unquote(msg)}")
    end
  end
  defmacrop warn(msg) do
    {current_state, _arity} = __CALLER__.function
    quote do
      Swarm.Logger.warn("[tracker:#{unquote(current_state)}] #{unquote(msg)}")
    end
  end
  defmacrop error(msg) do
    {current_state, _arity} = __CALLER__.function
    quote do
      Swarm.Logger.error("[tracker:#{unquote(current_state)}] #{unquote(msg)}")
    end
  end

  def start_link() do
    GenStateMachine.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    # Trap exits
    Process.flag(:trap_exit, true)
    # Start monitoring nodes
    :ok = :net_kernel.monitor_nodes(true, [node_type: :all])
    info "started"
    # wait for node list to populate
    nodelist = Enum.reject(Node.list(:connected), &ignore_node?/1)

    ring = Enum.reduce(nodelist, HashRing.new(Node.self), fn n, r ->
      HashRing.add_node(r, n)
    end)

    if Application.get_env(:swarm, :debug, false) do
      Task.start(fn -> :sys.trace(Swarm.Tracker, true) end)
    end

    timeout = Application.get_env(:swarm, :sync_nodes_timeout, @sync_nodes_timeout)
    Process.send_after(self(), :cluster_join, timeout)

    state = %TrackerState{nodes: nodelist, ring: ring, self: node()}

    {:ok, :cluster_wait, state}
  end

  def cluster_wait(:info, {:nodeup, node, _}, %TrackerState{} = state) do
    new_state = case nodeup(state, node) do
                  {:ok, new_state} -> new_state
                  {:ok, new_state, _next_state} -> new_state
                end
    {:keep_state, new_state}
  end
  def cluster_wait(:info, {:nodedown, node, _}, %TrackerState{} = state) do
    new_state = case nodedown(state, node) do
                    {:ok, new_state} -> new_state
                    {:ok, new_state, _next_state} -> new_state
                end
    {:keep_state, new_state}
  end
  def cluster_wait(:info, :cluster_join, %TrackerState{nodes: []} = state) do
    info "joining cluster.."
    info "no connected nodes, proceeding without sync"
    interval = Application.get_env(:swarm, :anti_entropy_interval, @default_anti_entropy_interval)
    Process.send_after(self(), :anti_entropy, interval)
    {:next_state, :tracking, %{state | clock: Clock.seed()}}
  end
  def cluster_wait(:info, :cluster_join, %TrackerState{nodes: nodes} = state) do
    info "joining cluster.."
    info "found connected nodes: #{inspect nodes}"
    # Connect to a random node and sync registries,
    # start anti-entropy, and start loop with forked clock of
    # remote node
    sync_node = Enum.random(nodes)
    info "selected sync node: #{sync_node}"
    # Send sync request
    ref = Process.monitor({__MODULE__, sync_node})
    GenStateMachine.cast({__MODULE__, sync_node}, {:sync, self()})
    {:next_state, :syncing, %{state | sync_node: sync_node, sync_ref: ref}}
  end
  def cluster_wait(:cast, {:sync, from}, %TrackerState{nodes: [from_node]} = state) when node(from) == from_node do
    info "joining cluster.."
    sync_node = node(from)
    info "syncing with #{sync_node}"
    ref = Process.monitor({__MODULE__, sync_node})
    clock = Clock.seed()
    GenStateMachine.cast(from, {:sync_recv, self(), clock, :ets.tab2list(:swarm_registry)})
    {:next_state, :awaiting_sync_ack, %{state | clock: clock, sync_node: sync_node, sync_ref: ref}}
  end
  def cluster_wait(:cast, {:sync, from}, %TrackerState{} = state) do
    info "pending sync request from #{node(from)}"
    {:keep_state, %{state | pending_sync_reqs: [from|state.pending_sync_reqs]}}
  end
  def cluster_wait(_event_type, _event_data, _state) do
    {:keep_state_and_data, :postpone}
  end

  def syncing(:info, {:nodeup, node, _}, %TrackerState{} = state) do
    new_state = case nodeup(state, node) do
                  {:ok, new_state} -> new_state
                  {:ok, new_state, _next_state} -> new_state
                end
    {:keep_state, new_state}
  end
  def syncing(:info, {:DOWN, ref, _type, _pid, _info}, %TrackerState{clock: clock, sync_ref: ref} = state) do
    info "the remote tracker we're syncing with has crashed, selecting a new one"
    case state.nodes -- [state.sync_node] do
      [] when clock == nil ->
        # there are no other nodes to select, we'll be the seed
        info "no other available nodes, becoming seed node"
        new_state = %{state | clock: Clock.seed(),
                              sync_node: nil,
                              sync_ref: nil}
        {:next_state, :tracking, new_state}
      [] ->
        info "no other available nodes, cancelling sync"
        new_state = %{state | sync_node: nil, sync_ref: nil}
        {:next_state, :tracking, new_state}
      new_nodes ->
        new_sync_node = Enum.random(new_nodes)
        info "selected sync node: #{new_sync_node}"
        # Send sync request
        ref = Process.monitor({__MODULE__, new_sync_node})
        GenStateMachine.cast({__MODULE__, new_sync_node}, {:sync, self()})
        new_state = %{state | sync_node: new_sync_node, sync_ref: ref}
        {:keep_state, new_state}
    end
  end
  def syncing(:info, {:nodedown, node, _}, %TrackerState{ring: ring, clock: clock, nodes: nodes, sync_node: node} = state) do
    info "the selected sync node #{node} went down, selecting new node"
    Process.demonitor(state.sync_ref, [:flush])
    case nodes -- [node] do
      [] when clock == nil ->
        # there are no other nodes to select, we'll be the seed
        info "no other available nodes, becoming seed node"
        new_state = %{state | nodes: [],
                              ring: HashRing.remove_node(ring, node),
                              clock: Clock.seed(),
                              sync_node: nil,
                              sync_ref: nil}
        {:next_state, :tracking, new_state}
      [] ->
        # there are no other nodes to select, nothing to do
        info "no other available nodes, cancelling sync"
        new_state = %{state | nodes: [],
                              ring: HashRing.remove_node(ring, node),
                              sync_node: nil,
                              sync_ref: nil}
        {:next_state, :tracking, new_state}
      new_nodes ->
        new_sync_node = Enum.random(new_nodes)
        info "selected sync node: #{new_sync_node}"
        # Send sync request
        ref = Process.monitor({__MODULE__, new_sync_node})
        GenStateMachine.cast({__MODULE__, new_sync_node}, {:sync, self()})
        new_state = %{state | nodes: new_nodes,
                              ring: HashRing.remove_node(ring, node),
                              sync_node: new_sync_node,
                              sync_ref: ref}
        {:keep_state, new_state}
    end
  end
  def syncing(:info, {:nodedown, node, _}, %TrackerState{} = state) do
    new_state = case nodedown(state, node) do
                  {:ok, new_state} -> new_state
                  {:ok, new_state, _next_state} -> new_state
                end
    {:keep_state, new_state}
  end
  def syncing(:cast, {:sync_begin_tiebreaker, from, rdie}, %TrackerState{sync_node: sync_node} = state) when node(from) == sync_node do
    # Break the tie and continue
    ldie = :rand.uniform(20)
    info "there is a tie between syncing nodes, breaking with die roll (#{ldie}).."
    GenStateMachine.cast(from, {:sync_end_tiebreaker, self(), rdie, ldie})
    cond do
      ldie > rdie or (rdie == ldie and Node.self > node(from)) ->
        info "we won the die roll (#{ldie} vs #{rdie}), sending registry.."
        # This is the new seed node
        {clock, rclock} = Clock.fork(state.clock || Clock.seed())
        GenStateMachine.cast(from, {:sync_recv, self(), rclock, :ets.tab2list(:swarm_registry)})
        {:next_state, :awaiting_sync_ack, %{state | clock: clock}}
      :else ->
        info "#{node(from)} won the die roll (#{rdie} vs #{ldie}), "
        # the other node wins the roll
        :keep_state_and_data
    end
  end
  def syncing(:cast, {:sync_end_tiebreaker, from, ldie, rdie}, %TrackerState{sync_node: sync_node} = state) when node(from) == sync_node do
    cond do
      rdie > ldie or (rdie == ldie and node(from) > Node.self) ->
        info "#{node(from)} won the die roll (#{rdie} vs #{ldie}), waiting for payload.."
        # The other node won the die roll, either by a greater die roll, or the absolute
        # tie breaker of node ordering
        :keep_state_and_data
      :else ->
        info "we won the die roll (#{ldie} vs #{rdie}), sending payload.."
        # This is the new seed node
        {clock, rclock} = Clock.fork(state.clock || Clock.seed())
        GenStateMachine.cast(from, {:sync_recv, self(), rclock, :ets.tab2list(:swarm_registry)})
        {:next_state, :awaiting_sync_ack, %{state | clock: clock}}
    end
  end
  # Successful first sync
  def syncing(:cast, {:sync_recv, from, clock, registry}, %TrackerState{clock: nil, sync_node: sync_node} = state)
    when node(from) == sync_node do
      info "received registry from #{node(from)}, loading.."
      # let remote node know we've got the registry
      GenStateMachine.cast(from, {:sync_ack, Node.self})
      # load target registry
      for entry(name: name, pid: pid, meta: meta, clock: clock) <- registry do
        ref = Process.monitor(pid)
        true = :ets.insert_new(:swarm_registry, entry(name: name, pid: pid, ref: ref, meta: meta, clock: clock))
      end
      # update our local clock to match remote clock
      state = %{state | clock: clock}
      info "finished sync, acknowledgement sent to #{node(from)}"
      # clear any pending sync requests to prevent blocking
      resolve_pending_sync_requests(state)
  end
  # Successful anti-entropy sync
  def syncing(:cast, {:sync_recv, from, _clock, registry}, %TrackerState{sync_node: sync_node} = state) when node(from) == sync_node do
    info "received registry from #{sync_node}, merging.."
    # let remote node know we've got the registry
    GenStateMachine.cast(from, {:sync_ack, Node.self})
    # map over the registry and check that all local entries are correct
    new_state = Enum.reduce(registry, state, fn
      entry(name: rname, pid: rpid, meta: rmeta, clock: rclock) = rreg, %TrackerState{clock: clock} = state ->
      case :ets.lookup(:swarm_registry, rname) do
        [] ->
          # missing local registration
          cond do
            Clock.leq(rclock, clock) ->
              # our clock is ahead, so we'll do nothing
              debug "local is missing #{inspect rname}, but local clock is dominant, ignoring"
              state
            :else ->
              # our clock is behind, so add the registration
              debug "local is missing #{inspect rname}, adding.."
              ref = Process.monitor(rpid)
              true = :ets.insert_new(:swarm_registry, entry(name: rname, pid: rpid, ref: ref, meta: rmeta, clock: rclock))
              %{state | clock: Clock.event(clock)}
          end
        [entry(pid: ^rpid, meta: ^rmeta, clock: ^rclock)] ->
          # this entry matches, nothing to do
          state
        [entry(pid: ^rpid, meta: ^rmeta, clock: _)] ->
          # the clocks differ, but the data is identical, so let's update it so they're the same
          :ets.update_element(:swarm_registry, rname, [{entry(:clock)+1, rclock}])
          state
        [entry(pid: ^rpid, meta: lmeta, clock: lclock)] ->
          # the metadata differs, we need to merge it
          cond do
            Clock.leq(lclock, rclock) ->
              # the remote clock dominates, so merge favoring data from the remote registry
              new_meta = Map.merge(lmeta, rmeta)
              :ets.update_element(:swarm_registry, rname, [{entry(:meta)+1, new_meta}])
              %{state | clock: Clock.event(clock)}
            Clock.leq(rclock, lclock) ->
              # the local clock dominates, so merge favoring data from the local registry
              new_meta = Map.merge(rmeta, lmeta)
              :ets.update_element(:swarm_registry, rname, [{entry(:meta)+1, new_meta}])
              %{state | clock: Clock.event(clock)}
            :else ->
              # the clocks conflict, but there's nothing we can do, so warn and then keep the local
              # view for the time being
              warn "local and remote metadata for #{inspect rname} conflicts, keeping local for now"
              state
          end
        [entry(pid: lpid, clock: lclock) = lreg] ->
          # there are two different processes for the same name, we need to resolve
          cond do
            Clock.leq(lclock, rclock) ->
              # the remote registration is newer
              resolve_incorrect_local_reg(sync_node, lreg, rreg, state)
            Clock.leq(rclock, lclock) ->
              # the local registration is newer
              debug "remote view of #{inspect rname} is outdated, resolving.."
              resolve_incorrect_remote_reg(sync_node, lreg, rreg, state)
            :else ->
              # the clocks conflict, determine which registration is correct based on current topology
              # and resolve the conflict
              rpid_node    = node(rpid)
              lpid_node    = node(lpid)
              target_node  = HashRing.key_to_node(state.ring, rname)
              cond do
                target_node == rpid_node and lpid_node != rpid_node ->
                  debug "remote and local view of #{inspect rname} conflict, but remote is correct, resolving.."
                  resolve_incorrect_local_reg(sync_node, lreg, rreg, state)
                target_node == lpid_node and rpid_node != lpid_node ->
                  debug "remote and local view of #{inspect rname} conflict, but local is correct, resolving.."
                  resolve_incorrect_remote_reg(sync_node, lreg, rreg, state)
                lpid_node == rpid_node && rpid > lpid ->
                  debug "remote and local view of #{inspect rname} conflict, " <>
                    "but the remote view is more recent, resolving.."
                  resolve_incorrect_local_reg(sync_node, lreg, rreg, state)
                lpid_node == rpid_node && lpid > rpid ->
                  debug "remote and local view of #{inspect rname} conflict, " <>
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
    info "synchronization complete!"
    resolve_pending_sync_requests(new_state)
  end
  def syncing(:cast, {:sync_recv, from, _clock, _registry}, _state) do
    # somebody is sending us a thing which expects an ack, but we no longer care about it
    # we should reply even though we're dropping this message
    GenStateMachine.cast(from, {:sync_ack, Node.self})
    :keep_state_and_data
  end
  def syncing(:cast, {:sync_err, from}, %TrackerState{nodes: nodes, sync_node: sync_node} = state) when node(from) == sync_node do
    Process.demonitor(state.sync_ref, [:flush])
    cond do
      # Something weird happened during sync, so try a different node,
      # with this implementation, we *could* end up selecting the same node
      # again, but that's fine as this is effectively a retry
      length(nodes) > 0 ->
        warn "a problem occurred during sync, choosing a new node to sync with"
        # we need to choose a different node to sync with and try again
        new_sync_node = Enum.random(nodes)
        ref = Process.monitor({__MODULE__, new_sync_node})
        GenStateMachine.cast({__MODULE__, new_sync_node}, {:sync, self()})
        {:keep_state, %{state | sync_node: new_sync_node, sync_ref: ref}}
      # Something went wrong during sync, but there are no other nodes to sync with,
      # not even the original sync node (which probably implies it shutdown or crashed),
      # so we're the sync node now
      :else ->
        warn "a problem occurred during sync, but no other available sync targets, becoming seed node"
        {:next_state, :tracking, %{state | pending_sync_reqs: [], sync_node: nil, sync_ref: nil, clock: Clock.seed()}}
    end
  end
  def syncing(:cast, {:sync, from}, %TrackerState{sync_node: sync_node}) when node(from) == sync_node do
    # Incoming sync request from our target node, in other words, A is trying to sync with B,
    # and B is trying to sync with A - we need to break the deadlock
    # 20d, I mean why not?
    die = :rand.uniform(20)
    info "there is a tie between syncing nodes, breaking with die roll (#{die}).."
    GenStateMachine.cast(from, {:sync_begin_tiebreaker, self(), die})
    :keep_state_and_data
  end
  def syncing(:cast, {:sync, from}, %TrackerState{} = state) do
    info "pending sync request from #{node(from)}"
    new_pending_reqs = Enum.uniq([from|state.pending_sync_reqs])
    {:keep_state, %{state | pending_sync_reqs: new_pending_reqs}}
  end
  def syncing(_event_type, _event_data, _state) do
    {:keep_state_and_data, :postpone}
  end

  defp resolve_pending_sync_requests(%TrackerState{pending_sync_reqs: []} = state) do
    info "pending sync requests cleared"
    case state.sync_ref do
      nil -> :ok
      ref -> Process.demonitor(ref, [:flush])
    end
    {:next_state, :tracking, %{state | sync_node: nil, sync_ref: nil}}
  end
  defp resolve_pending_sync_requests(%TrackerState{pending_sync_reqs: [pid|pending]} = state) do
    pending_node = node(pid)
    # Remove monitoring of the previous sync node
    case state.sync_ref do
      nil -> :ok
      ref -> Process.demonitor(ref, [:flush])
    end
    cond do
      Enum.member?(state.nodes, pending_node) ->
        info "clearing pending sync request for #{pending_node}"
        {lclock, rclock} = Clock.fork(state.clock)
        ref = Process.monitor(pid)
        GenStateMachine.cast(pid, {:sync_recv, self(), rclock, :ets.tab2list(:swarm_registry)})
        new_state = %{state | sync_node: node(pid),
                              sync_ref: ref,
                              pending_sync_reqs: pending,
                              clock: lclock}
        {:next_state, :awaiting_sync_ack, new_state}
      :else ->
        resolve_pending_sync_requests(%{state | sync_node: nil, sync_ref: nil, pending_sync_reqs: pending})
    end
  end

  def awaiting_sync_ack(:cast, {:sync_ack, sync_node}, %TrackerState{sync_node: sync_node} = state) do
    info "received sync acknowledgement from #{sync_node}"
    Process.demonitor(state.sync_ref, [:flush])
    resolve_pending_sync_requests(%{state | sync_node: nil, sync_ref: nil})
  end
  def awaiting_sync_ack(:info, {:DOWN, ref, _type, _pid, _info}, %TrackerState{sync_ref: ref} = state) do
    warn "wait for acknowledgement from #{state.sync_node} cancelled, tracker down"
    resolve_pending_sync_requests(%{state | sync_node: nil, sync_ref: nil})
  end
  def awaiting_sync_ack(:info, {:nodeup, node, _}, %TrackerState{} = state) do
    new_state = case nodeup(state, node) do
                  {:ok, new_state} -> new_state
                  {:ok, new_state, _next_state} -> new_state
                end
    {:keep_state, new_state}
  end
  def awaiting_sync_ack(:info, {:nodedown, node, _}, %TrackerState{sync_node: node} = state) do
    new_state = case nodedown(state, node) do
                  {:ok, new_state} -> new_state
                  {:ok, new_state, _next_state} -> new_state
                end
    Process.demonitor(state.sync_ref, [:flush])
    resolve_pending_sync_requests(%{new_state | sync_node: nil, sync_ref: nil})
  end
  def awaiting_sync_ack(:info, {:nodedown, node, _}, %TrackerState{} = state) do
    new_state = case nodedown(state, node) do
                  {:ok, new_state} -> new_state
                  {:ok, new_state, _next_state} -> new_state
                end
    {:keep_state, new_state}
  end
  def awaiting_sync_ack(:cast, {:sync_recv, from, _clock, _registry}, _state) do
    # somebody is sending us a thing which expects an ack,
    # we should reply even though we're dropping this message
    GenStateMachine.cast(from, {:sync_ack, Node.self})
    :keep_state_and_data
  end
  def awaiting_sync_ack(_event_type, _event_data, _state) do
    {:keep_state_and_data, :postpone}
  end

  def tracking(:info, {:EXIT, _child, _reason}, _state) do
    # A child process started by this tracker has crashed
    :keep_state_and_data
  end
  def tracking(:info, {:nodeup, node, _}, state) do
    case nodeup(state, node) do
      {:ok, new_state} ->
        {:keep_state, new_state}
      {:ok, new_state, {:topology_change, change_info}} ->
        handle_topology_change(change_info, new_state)
    end
  end
  def tracking(:info, {:nodedown, node, _}, state) do
    case nodedown(state, node) do
      {:ok, new_state} ->
        {:keep_state, new_state}
      {:ok, new_state, {:topology_change, change_info}} ->
        handle_topology_change(change_info, new_state)
    end
  end
  def tracking(:info, :anti_entropy, state) do
    anti_entropy(state)
  end
  # A change event received from another replica/node
  def tracking(:cast, {:event, from, rclock, event}, state) do
    handle_replica_event(from, event, rclock, state)
  end
  # Received a handoff request from a node
  def tracking(:cast, {:handoff, from, {name, m, f, a, handoff_state, rclock}}, state) do
    handle_handoff(from, {name, m, f, a, handoff_state, rclock}, state)
  end
  # A remote registration failed due to nodedown during the call
  def tracking(:cast, {:retry, from, {:track, name, m, f, a}}, state) do
    handle_retry(from, {:track, name, m, f, a}, state)
  end
  # A change event received locally
  def tracking({:call, from}, msg, state) do
    handle_call(msg, from, state)
  end
  def tracking(:cast, msg, state) do
    handle_cast(msg, state)
  end
  # A tracked process has gone down
  def tracking(:info, {:DOWN, ref, _type, pid, info}, state) do
    handle_monitor(ref, pid, info, state)
  end
  def tracking(event_type, event_data, state) do
    handle_event(event_type, event_data, state)
  end

  # This state helps us ensure that nodes proactively keep themselves synced
  # after joining the cluster and initial syncrhonization. This way if replication
  # events fail for some reason, we can control the drift in registry state
  def anti_entropy(%TrackerState{nodes: []}) do
    interval = Application.get_env(:swarm, :anti_entropy_interval, @default_anti_entropy_interval)
    Process.send_after(self(), :anti_entropy, interval)
    :keep_state_and_data
  end
  def anti_entropy(%TrackerState{nodes: nodes} = state) do
    sync_node = Enum.random(nodes)
    info "syncing with #{sync_node}"
    ref = Process.monitor({__MODULE__, sync_node})
    GenStateMachine.cast({__MODULE__, sync_node}, {:sync, self()})
    new_state = %{state | sync_node: sync_node, sync_ref: ref}
    interval = Application.get_env(:swarm, :anti_entropy_interval, @default_anti_entropy_interval)
    Process.send_after(self(), :anti_entropy, interval)
    {:next_state, :syncing, new_state}
  end


  # This message is sent as a broadcast message for replication
  def handle_event(:info, {:event, from, rclock, event}, state) do
    handle_replica_event(from, event, rclock, state)
  end
  # If we receive cluster_join outside of cluster_wait it's because
  # we implicitly joined the cluster due to a sync event, we know if
  # we receive such an event the cluster is already formed due to how
  # Erlang distribution works (it's a mesh)
  def handle_event(:info, :cluster_join, _state) do
    :keep_state_and_data
  end
  def handle_event({:call, from}, msg, state) do
    handle_call(msg, from, state)
  end
  def handle_event(:cast, msg, state) do
    handle_cast(msg, state)
  end
  # Default event handler
  def handle_event(event_type, event_data, _state) do
    debug "unexpected event: #{inspect {event_type, event_data}}"
    :keep_state_and_data
  end

  def code_change(_oldvsn, state, data, _extra) do
    {:ok, state, data}
  end

  # This is the callback for when a process is being handed off from a remote node to this node.
  defp handle_handoff(_from, {name, m, f, a, handoff_state, _rclock}, %TrackerState{clock: clock} = state) do
    try do
      # If a network split is being healed, we almost certainly will have a
      # local registration already for this name (since it was present on this side of the split)
      # If not, we'll restart it, but if so, we'll send the handoff state to the old process and
      # let it determine how to resolve the conflict
      current_node = Node.self
      case Registry.get_by_name(name) do
        :undefined ->
          {:ok, pid} = apply(m, f, a)
          GenServer.cast(pid, {:swarm, :end_handoff, handoff_state})
          ref = Process.monitor(pid)
          new_clock = Clock.event(clock)
          meta = %{mfa: {m,f,a}}
          true = :ets.insert_new(:swarm_registry, entry(name: name, pid: pid, ref: ref, meta: meta, clock: Clock.peek(new_clock)))
          broadcast_event(state.nodes, Clock.peek(new_clock), {:track, name, pid, meta})
          {:keep_state, %{state | clock: new_clock}}
        entry(pid: pid) when node(pid) == current_node ->
          GenServer.cast(pid, {:swarm, :resolve_conflict, handoff_state})
          new_clock = Clock.event(clock)
          meta = %{mfa: {m,f,a}}
          broadcast_event(state.nodes, Clock.peek(new_clock), {:track, name, pid, meta})
          {:keep_state, %{state | clock: new_clock}}
      end
    catch
      kind, err ->
        error Exception.format(kind, err, System.stacktrace)
        :keep_state_and_data
    end
  end

  # This is the callback for when a nodeup/down event occurs after the tracker has entered
  # the main receive loop. Topology changes are handled a bit differently during startup.
  defp handle_topology_change({type, remote_node}, %TrackerState{} = state) do
    debug "topology change (#{type} for #{remote_node})"
    current_node = Node.self
    new_clock = :ets.foldl(fn
      entry(name: name, pid: pid, meta: %{mfa: {m,f,a}}) = obj, lclock when node(pid) == current_node ->
        case HashRing.key_to_node(state.ring, name) do
          ^current_node ->
            # This process is correct
            lclock
          other_node ->
            debug "#{inspect pid} belongs on #{other_node}"
            # This process needs to be moved to the new node
            try do
              case GenServer.call(pid, {:swarm, :begin_handoff}) do
                :ignore ->
                  debug "#{inspect name} has requested to be ignored"
                  lclock
                {:resume, handoff_state} ->
                  debug "#{inspect name} has requested to be resumed"
                  {:ok, state} = remove_registration(obj, %{state | clock: lclock})
                  send(pid, {:swarm, :die})
                  debug "sending handoff for #{inspect name} to #{other_node}"
                  GenStateMachine.cast({__MODULE__, other_node},
                                       {:handoff, self(), {name, m, f, a, handoff_state, Clock.peek(state.clock)}})
                  state.clock
                :restart ->
                  debug "#{inspect name} has requested to be restarted"
                  {:ok, new_state} = remove_registration(obj, %{state | clock: lclock})
                  send(pid, {:swarm, :die})
                  case handle_call({:track, name, m, f, a}, nil, %{state | clock: lclock}) do
                    :keep_state_and_data ->
                      new_state.clock
                    {:keep_state, new_state} ->
                      new_state.clock
                  end
              end
            catch
              _, err ->
                warn "handoff failed for #{inspect name}: #{inspect err}"
                lclock
            end
        end
      entry(name: name, pid: pid, meta: %{mfa: {m,f,a}}) = obj, lclock ->
        cond do
          Enum.member?(state.nodes, node(pid)) ->
            # the parent node is still up
            lclock
          :else ->
            # pid is dead, we're going to restart it
            case HashRing.key_to_node(state.ring, name) do
              ^current_node ->
                debug "restarting #{inspect name} on #{current_node}"
                {:ok, new_state} = remove_registration(obj, %{state | clock: lclock})
                case handle_call({:track, name, m, f, a}, nil, new_state) do
                  :keep_state_and_data ->
                    new_state.clock
                  {:keep_state, new_state} ->
                    new_state.clock
                end
              _other_node ->
                # other_node will tell us to unregister/register the restarted pid
                lclock
            end
        end
      entry(name: name, pid: pid) = obj, lclock ->
        cond do
          Enum.member?(state.nodes, node(pid)) ->
            # the parent node is still up
            lclock
          :else ->
            # the parent node is down, but we cannot restart this pid, so unregister it
            debug "removing registration for #{inspect name}, #{node(pid)} is down"
            {:ok, new_state} = remove_registration(obj, %{state | clock: lclock})
            new_state.clock
        end
    end, state.clock, :swarm_registry)
    info "topology change complete"
    {:keep_state, %{state | clock: new_clock}}
  end

  # This is the callback for tracker events which are being replicated from other nodes in the cluster
  defp handle_replica_event(_from, {:track, name, pid, meta}, rclock, %TrackerState{clock: clock} = state) do
    debug "replicating registration for #{inspect name} (#{inspect pid}) locally"
    case Registry.get_by_pid_and_name(pid, name) do
      entry(name: ^name, pid: ^pid, meta: ^meta) ->
        # We're already up to date
        :keep_state_and_data
      entry(name: ^name, pid: ^pid, meta: lmeta) ->
        # We don't have the same view of the metadata
        cond do
          Clock.leq(clock, rclock) ->
            # The remote version is dominant
            new_meta = Map.merge(lmeta, meta)
            :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}])
            {:keep_state, %{state | clock: Clock.event(clock)}}
          Clock.leq(rclock, clock) ->
            # The local version is dominant
            new_meta = Map.merge(meta, lmeta)
            :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}])
            {:keep_state, %{state | clock: Clock.event(clock)}}
          :else ->
            warn "received track event for #{inspect name}, but local clock conflicts with remote clock, event unhandled"
            :keep_state_and_data
        end
      :undefined ->
        ref = Process.monitor(pid)
        true = :ets.insert_new(:swarm_registry, entry(name: name, pid: pid, ref: ref, meta: meta, clock: rclock))
        {:keep_state, %{state | clock: Clock.event(clock)}}
    end
  end
  defp handle_replica_event(_from, {:untrack, pid}, rclock, %TrackerState{clock: clock} = state) do
    debug "replica event: untrack #{inspect pid}"
    case Registry.get_by_pid(pid) do
      :undefined ->
        :keep_state_and_data
      entries when is_list(entries) ->
        new_clock = Enum.reduce(entries, clock, fn entry(ref: ref, clock: lclock) = obj, nclock ->
          cond do
            Clock.leq(lclock, rclock) ->
              # registration came before unregister, so remove the registration
              Process.demonitor(ref, [:flush])
              :ets.delete_object(:swarm_registry, obj)
              Clock.event(nclock)
            Clock.leq(rclock, lclock) ->
              # registration is newer than de-registration, ignore msg
              debug "untrack is causally dominated by track for #{inspect pid}, ignoring.."
              nclock
            :else ->
              debug "untrack is causally conflicted with track for #{inspect pid}, ignoring.."
              nclock
          end
        end)
        {:keep_state, %{state | clock: new_clock}}
    end
  end
  defp handle_replica_event(_from, {:add_meta, key, value, pid}, rclock, %TrackerState{clock: clock} = state) do
    debug "replica event: add_meta #{inspect {key, value}} to #{inspect pid}"
    case Registry.get_by_pid(pid) do
      :undefined ->
        :keep_state_and_data
      entries when is_list(entries) ->
        new_clock = Enum.reduce(entries, clock, fn entry(name: name, meta: old_meta, clock: lclock), nclock ->
          cond do
            Clock.leq(lclock, rclock) ->
              new_meta = Map.put(old_meta, key, value)
              :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, rclock}])
              nclock = Clock.event(nclock)
            Clock.leq(rclock, lclock) ->
              cond do
                Map.has_key?(old_meta, key) ->
                  debug "request to add meta to #{inspect pid} (#{inspect {key, value}}) is causally dominated by local, ignoring.."
                  nclock
                :else ->
                  new_meta = Map.put(old_meta, key, value)
                  :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, rclock}])
                  nclock = Clock.event(nclock)
              end
            :else ->
              # we're going to take the last-writer wins approach for resolution for now
              new_meta = Map.merge(old_meta, %{key => value})
              # we're going to keep our local clock though and re-broadcast the update to ensure we converge
              nclock = Clock.event(clock)
              :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, Clock.peek(nclock)}])
              debug "conflicting meta for #{inspect name}, updating and notifying other nodes"
              broadcast_event(state.nodes, Clock.peek(nclock), {:update_meta, new_meta, pid})
              nclock
          end
        end)
        {:keep_state, %{state | clock: new_clock}}
    end
  end
  defp handle_replica_event(_from, {:update_meta, new_meta, pid}, rclock, %TrackerState{clock: clock} = state) do
    debug "replica event: update_meta #{inspect new_meta} for #{inspect pid}"
    case Registry.get_by_pid(pid) do
      :undefined ->
        :keep_state_and_data
      entries when is_list(entries) ->
        new_clock = Enum.reduce(entries, clock, fn entry(name: name, meta: old_meta, clock: lclock), nclock ->
          cond do
            Clock.leq(lclock, rclock) ->
              meta = Map.merge(old_meta, new_meta)
              :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, meta}, {entry(:clock)+1, rclock}])
              Clock.event(nclock)
            Clock.leq(rclock, lclock) ->
              meta = Map.merge(new_meta, old_meta)
              :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, meta}, {entry(:clock)+1, rclock}])
              Clock.event(nclock)
            :else ->
              # we're going to take the last-writer wins approach for resolution for now
              new_meta = Map.merge(old_meta, new_meta)
              # we're going to keep our local clock though and re-broadcast the update to ensure we converge
              nclock = Clock.event(nclock)
              :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, Clock.peek(nclock)}])
              debug "conflicting meta for #{inspect name}, updating and notifying other nodes"
              broadcast_event(state.nodes, Clock.peek(nclock), {:update_meta, new_meta, pid})
              nclock
          end
        end)
        {:keep_state, %{state | clock: new_clock}}
    end
  end
  defp handle_replica_event(_from, {:remove_meta, key, pid}, rclock, %TrackerState{clock: clock} = state) do
    debug "replica event: remove_meta #{inspect key} from #{inspect pid}"
    case Registry.get_by_pid(pid) do
      :undefined ->
        :keep_state_and_data
      entries when is_list(entries) ->
        new_clock = Enum.reduce(entries, clock, fn entry(name: name, meta: meta, clock: lclock), nclock ->
          cond do
            Clock.leq(lclock, rclock) ->
              new_meta = Map.drop(meta, [key])
              :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, rclock}])
              Clock.event(nclock)
            Clock.leq(rclock, lclock) and Map.has_key?(meta, key) ->
              # ignore the request, as the local clock dominates the remote
              nclock
            Clock.leq(rclock, lclock) ->
              # local dominates the remote, but the key is not present anyway
              nclock
            Map.has_key?(meta, key) ->
              warn "received remove_meta event, but local clock conflicts with remote clock, event unhandled"
              nclock
            :else ->
              nclock
          end
        end)
        {:keep_state, %{state | clock: new_clock}}
    end
  end
  defp handle_replica_event(_from, event, _clock, _state) do
    warn "received unrecognized replica event: #{inspect event}"
    :keep_state_and_data
  end

  # This is the handler for local operations on the tracker which require a response.
  defp handle_call({:whereis, name}, from, %TrackerState{ring: ring}) do
    current_node = Node.self
    case HashRing.key_to_node(ring, name) do
      ^current_node ->
        case Registry.get_by_name(name) do
          :undefined ->
            GenStateMachine.reply(from, :undefined)
          entry(pid: pid) ->
            GenStateMachine.reply(from, pid)
        end
      other_node ->
        Task.Supervisor.start_child(Swarm.TaskSupervisor, fn ->
          case :rpc.call(other_node, Swarm.Registry, :get_by_name, [name], :infinity) do
            :undefined ->
              GenStateMachine.reply(from, :undefined)
            entry(pid: pid) ->
              GenStateMachine.reply(from, pid)
            {:badrpc, reason} ->
              warn "failed to execute remote get_by_name on #{inspect other_node}: #{inspect reason}"
              GenStateMachine.reply(from, :undefined)
          end
        end)
    end
    :keep_state_and_data
  end
  defp handle_call({:track, name, pid, meta}, from, %TrackerState{} = state) do
    debug "registering #{inspect pid} as #{inspect name}, with metadata #{inspect meta}"
    add_registration({name, pid, meta}, from, state)
  end
  defp handle_call({:track, name, m, f, a}, from, %TrackerState{ring: ring} = state) do
    current_node = Node.self
    case from do
      {from_pid, _} when node(from_pid) != current_node ->
        debug "#{inspect node(from_pid)} is registering #{inspect name} as process started by #{m}.#{f}/#{length(a)} with args #{inspect a}"
      _ ->
        debug "registering #{inspect name} as process started by #{m}.#{f}/#{length(a)} with args #{inspect a}"
    end
    case HashRing.key_to_node(ring, name) do
      ^current_node ->
        case Registry.get_by_name(name) do
          :undefined ->
            debug "starting #{inspect name} on #{current_node}"
            try do
              case apply(m, f, a) do
                {:ok, pid} ->
                  debug "started #{inspect name} on #{current_node}"
                  add_registration({name, pid, %{mfa: {m,f,a}}}, from, state)
                err when from != nil ->
                  warn "failed to start #{inspect name} on #{current_node}: #{inspect err}"
                  GenStateMachine.reply(from, {:error, {:invalid_return, err}})
                  :keep_state_and_data
                err ->
                  warn "failed to start #{inspect name} on #{current_node}: #{inspect err}"
                  :keep_state_and_data
              end
            catch
              kind, reason when from != nil ->
                warn Exception.format(kind, reason, System.stacktrace)
                GenStateMachine.reply(from, {:error, reason})
                :keep_state_and_data
              kind, reason ->
                warn Exception.format(kind, reason, System.stacktrace)
                :keep_state_and_data
            end
          entry(pid: pid) when from != nil ->
            debug "found #{inspect name} already registered on #{node(pid)}"
            GenStateMachine.reply(from, {:error, {:already_registered, pid}})
            :keep_state_and_data
          entry(pid: pid) ->
            debug "found #{inspect name} already registered on #{node(pid)}"
            :keep_state_and_data
        end
      remote_node ->
        {:ok, _pid} = Task.start(fn ->
          debug "starting #{inspect name} on #{remote_node}"
          start_pid_remotely(remote_node, from, name, m, f, a, state)
        end)
        :keep_state_and_data
    end
  end
  defp handle_call({:untrack, pid}, from, %TrackerState{} = state) do
    debug "untrack #{inspect pid}"
    {:ok, new_state} = remove_registration_by_pid(pid, state)
    GenStateMachine.reply(from, :ok)
    {:keep_state, new_state}
  end
  defp handle_call({:add_meta, key, value, pid}, from, %TrackerState{} = state) do
    debug "add_meta #{inspect {key, value}} to #{inspect pid}"
    {:ok, new_state} = add_meta_by_pid({key, value}, pid, state)
    GenStateMachine.reply(from, :ok)
    {:keep_state, new_state}
  end
  defp handle_call({:remove_meta, key, pid}, from, %TrackerState{} = state) do
    debug "remote_meta #{inspect key} for #{inspect pid}"
    {:ok, new_state} = remove_meta_by_pid(key, pid, state)
    GenStateMachine.reply(from, :ok)
    {:keep_state, new_state}
  end
  defp handle_call(msg, _from, _state) do
    warn "unrecognized call: #{inspect msg}"
    :keep_state_and_data
  end

  # This is the handler for local operations on the tracker which are asynchronous
  defp handle_cast({:sync, from}, %TrackerState{clock: clock} = state) do
    debug "received sync request from #{node(from)}"
    {lclock, rclock} = Clock.fork(clock)
    sync_node = node(from)
    ref = Process.monitor(from)
    GenStateMachine.cast(from, {:sync_recv, self(), rclock, :ets.tab2list(:swarm_registry)})
    {:next_state, :awaiting_sync_ack, %{state | clock: lclock, sync_node: sync_node, sync_ref: ref}}
  end
  defp handle_cast({:sync_recv, from, _clock, _registry}, _state) do
    # somebody is sending us a thing which expects an ack, but we no longer care about it
    # we should reply even though we're dropping this message
    GenStateMachine.cast(from, {:sync_ack, Node.self})
    :keep_state_and_data
  end
  defp handle_cast(msg, _state) do
    warn "unrecognized cast: #{inspect msg}"
    :keep_state_and_data
  end

  # This is only ever called if a registration needs to be sent to a remote node
  # and that node went down in the middle of the call to it's Swarm process.
  # We need to process the nodeup/down events by re-entering the receive loop first,
  # so we send ourselves a message to retry, this is the handler for that message
  defp handle_retry(from, {:track, name, m, f, a}, state) do
    handle_call({:track, name, m, f, a}, from, state)
  end
  defp handle_retry(_from, _event, _state) do
    :keep_state_and_data
  end

  # Called when a pid dies, and the monitor is triggered
  defp handle_monitor(ref, pid, :noconnection, %TrackerState{nodes: nodes, ring: ring} = state) do
    # lost connection to the node this pid is running on, check if we should restart it
    case Registry.get_by_ref(ref) do
      :undefined ->
        debug "lost connection to #{inspect pid}, but no registration could be found, ignoring.."
        :keep_state_and_data
      entry(name: name, pid: ^pid, meta: %{mfa: {m,f,a}}) = obj ->
        debug "lost connection to #{inspect name} (#{inspect pid}) on #{node(pid)}, node is down"
        current_node = Node.self
        # this event may have occurred before we get a nodedown event, so
        # for the purposes of this handler, preemptively remove the node from the
        # ring when calculating the new node
        pid_node = node(pid)
        ring  = HashRing.remove_node(ring, pid_node)
        state = %{state | nodes: nodes -- [pid_node], ring: ring}
        case HashRing.key_to_node(ring, name) do
          ^current_node ->
            debug "restarting #{inspect name} (#{inspect pid}) on #{current_node}"
            {:ok, new_state} = remove_registration(obj, state)
            handle_call({:track, name, m, f, a}, nil, new_state)
          other_node ->
            debug "#{inspect name} (#{inspect pid}) is owned by #{other_node}, skipping"
            {:ok, new_state} = remove_registration(obj, state)
            {:keep_state, new_state}
        end
      entry(pid: ^pid) = obj ->
        debug "lost connection to #{inspect pid}, but not restartable, removing registration.."
        {:ok, new_state} = remove_registration(state, obj)
        {:keep_state, new_state}
    end
  end
  defp handle_monitor(ref, pid, reason, %TrackerState{} = state) do
    case Registry.get_by_ref(ref) do
      :undefined ->
        debug "#{inspect pid} is down: #{inspect reason}, but no registration found, ignoring.."
        :keep_state_and_data
      entry(name: name, pid: ^pid) = obj ->
        debug "#{inspect name} is down: #{inspect reason}"
        {:ok, new_state} = remove_registration(obj, state)
        {:keep_state, new_state}
    end
  end

  # Starts a process on a remote node. Handles failures with a retry mechanism
  defp start_pid_remotely(remote_node, from, name, m, f, a, %TrackerState{} = state) do
    try do
      case GenStateMachine.call({__MODULE__, remote_node}, {:track, name, m, f, a}, :infinity) do
        {:ok, pid} ->
          debug "remotely started #{inspect name} (#{inspect pid}) on #{remote_node}"
          case from do
            nil -> :ok
            _   -> GenStateMachine.reply(from, {:ok, pid})
          end
        {:error, {:already_registered, pid}} ->
          debug "#{inspect name} already registered to #{inspect pid} on #{node(pid)}"
          case from do
            nil -> :ok
            _   -> GenStateMachine.reply(from, {:ok, pid})
          end
        {:error, _reason} = err when from != nil ->
          warn "#{inspect name} could not be started on #{remote_node}: #{inspect err}"
          GenStateMachine.reply(from, err)
        {:error, _reason} = err ->
          warn "#{inspect name} could not be started on #{remote_node}: #{inspect err}"
      end
    catch
      _, {:noproc, _} ->
        warn "remote tracker on #{remote_node} went down during registration, retrying operation.."
        start_pid_remotely(remote_node, from, name, m, f, a, state)
      _, {{:nodedown, _}, _} ->
        warn "failed to start #{inspect name} on #{remote_node}: nodedown, retrying operation.."
        new_state = %{state | nodes: state.nodes -- [remote_node], ring: HashRing.remove_node(state.ring, remote_node)}
        new_node = HashRing.key_to_node(new_state.ring, name)
        start_pid_remotely(new_node, from, name, m, f, a, new_state)
      kind, err when from != nil ->
        error Exception.format(kind, err, System.stacktrace)
        warn "failed to start #{inspect name} on #{remote_node}: #{inspect err}"
        GenStateMachine.reply(from, {:error, err})
      kind, err ->
        error Exception.format(kind, err, System.stacktrace)
        warn "failed to start #{inspect name} on #{remote_node}: #{inspect err}"
    end
  end

  ## Internal helpers

  defp broadcast_event([], _clock, _event),  do: :ok
  defp broadcast_event(nodes, clock, event) do
    case :rpc.sbcast(nodes, __MODULE__, {:event, self(), clock, event}) do
      {_good, []}  -> :ok
      {_good, bad_nodes} ->
        warn "broadcast of event (#{inspect event}) was not recevied by #{inspect bad_nodes}"
        :ok
    end
  end

  # Add a registration and reply to the caller with the result, then return the state transition
  defp add_registration({_name, _pid, _meta} = reg, from, state) do
    case register(reg, state) do
      {:ok, reply, new_state} when from != nil ->
        GenStateMachine.reply(from, {:ok, reply})
        {:keep_state, new_state}
      {:error, reply, new_state} when from != nil ->
        GenStateMachine.reply(from, {:error, reply})
        {:keep_state, new_state}
      {_type, _reply, new_state} ->
        {:keep_state, new_state}
    end
  end

  # Add a registration and return the result of the add
  defp register({name, pid, meta}, %TrackerState{clock: clock, nodes: nodes} = state) do
    case Registry.get_by_name(name) do
      :undefined ->
        ref = Process.monitor(pid)
        clock = Clock.event(clock)
        true = :ets.insert_new(:swarm_registry, entry(name: name, pid: pid, ref: ref, meta: meta, clock: Clock.peek(clock)))
        broadcast_event(nodes, Clock.peek(clock), {:track, name, pid, meta})
        {:ok, pid, %{state | clock: clock}}
      entry(pid: ^pid) ->
        # Not sure how this could happen, but hey, no need to return an error
        {:ok, pid, state}
      entry(pid: other_pid) ->
        debug "conflicting registration for #{inspect name}: remote (#{inspect pid}) vs. local #{inspect other_pid}"
        # Since there is already a registration, we need to check whether to kill the newly
        # created process
        pid_node = node(pid)
        current_node = Node.self
        case meta do
          %{mfa: _} when pid_node == current_node ->
            # This was created via register_name/4, which means we need to kill the pid we started
            Process.exit(pid, :kill)
          _ ->
            # This was a pid started by something else, so we can ignore it
            :ok
        end
        {:error, {:already_registered, other_pid}, state}
    end
  end

  # Remove a registration, and return the result of the remove
  defp remove_registration(entry(pid: pid, ref: ref) = obj, %TrackerState{clock: clock} = state) do
    Process.demonitor(ref, [:flush])
    :ets.delete_object(:swarm_registry, obj)
    clock = Clock.event(clock)
    broadcast_event(state.nodes, Clock.peek(clock), {:untrack, pid})
    {:ok, %{state | clock: clock}}
  end

  defp remove_registration_by_pid(pid, %TrackerState{clock: clock} = state) do
    case Registry.get_by_pid(pid) do
      :undefined ->
        broadcast_event(state.nodes, Clock.peek(clock), {:untrack, pid})
        {:ok, state}
      entries when is_list(entries) ->
        new_clock = Enum.reduce(entries, clock, fn entry(ref: ref) = obj, nclock ->
          Process.demonitor(ref, [:flush])
          :ets.delete_object(:swarm_registry, obj)
          nclock = Clock.event(nclock)
          broadcast_event(state.nodes, Clock.peek(nclock), {:untrack, pid})
          nclock
        end)
        {:ok, %{state | clock: new_clock}}
    end
  end

  defp add_meta_by_pid({key, value}, pid, %TrackerState{clock: clock} = state) do
    case Registry.get_by_pid(pid) do
      :undefined ->
        broadcast_event(state.nodes, Clock.peek(clock), {:add_meta, key, value, pid})
        {:ok, state}
      entries when is_list(entries) ->
        new_clock = Enum.reduce(entries, clock, fn entry(name: name, meta: old_meta), nclock ->
          new_meta = Map.put(old_meta, key, value)
          nclock = Clock.event(nclock)
          :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, Clock.peek(nclock)}])
          broadcast_event(state.nodes, Clock.peek(nclock), {:update_meta, new_meta, pid})
          nclock
        end)
        {:ok, %{state | clock: new_clock}}
    end
  end

  defp remove_meta_by_pid(key, pid, %TrackerState{clock: clock} = state) do
    case Registry.get_by_pid(pid) do
      :undefined ->
        broadcast_event(state.nodes, Clock.peek(clock), {:remove_meta, key, pid})
      entries when is_list(entries) ->
        new_clock = Enum.reduce(entries, clock, fn entry(name: name, meta: old_meta), nclock ->
          new_meta = Map.drop(old_meta, [key])
          nclock = Clock.event(nclock)
          :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, Clock.peek(nclock)}])
          broadcast_event(state.nodes, Clock.peek(nclock), {:update_meta, new_meta, pid})
          nclock
        end)
        {:ok, %{state | clock: new_clock}}
    end
  end

  @default_blacklist [~r/^remsh.*$/, ~r/^.+_upgrader_.+$/, ~r/^.+_maint_.+$/]
  # The list of configured ignore patterns for nodes
  # This is only applied if no whitelist is provided.
  defp node_blacklist(), do: Application.get_env(:swarm, :node_blacklist, @default_blacklist)
  # The list of configured whitelist patterns for nodes
  # If a whitelist is provided, any nodes which do not match the whitelist are ignored
  defp node_whitelist(), do: Application.get_env(:swarm, :node_whitelist, [])

  # Determine if a node should be ignored, even if connected
  # The whitelist and blacklist can contain literal strings, regexes, or regex strings
  # By default, all nodes are allowed, except those which are remote shell sessions
  # where the node name of the remote shell starts with `remsh` (relx, exrm, and distillery)
  # all use that prefix for remote shells.
  defp ignore_node?(node) do
    blacklist = node_blacklist()
    whitelist = node_whitelist()
    HashRing.Utils.ignore_node?(node, blacklist, whitelist)
  end

  # Used during anti-entropy checks to remove local registrations and replace them with the remote version
  defp resolve_incorrect_local_reg(_remote_node, entry(pid: lpid) = lreg, entry(name: rname, pid: rpid, meta: rmeta, clock: rclock), state) do
    # the remote registration is correct
    {:ok, new_state} = remove_registration(lreg, state)
    send(lpid, {:swarm, :die})
    # add the remote registration
    ref = Process.monitor(rpid)
    clock = Clock.event(new_state.clock)
    true = :ets.insert_new(:swarm_registry, entry(name: rname, pid: rpid, ref: ref, meta: rmeta, clock: rclock))
    %{new_state | clock: clock}
  end

  # Used during anti-entropy checks to remove remote registrations and replace them with the local version
  defp resolve_incorrect_remote_reg(remote_node, entry(pid: lpid, meta: lmeta), entry(name: rname, pid: rpid), state) do
    GenStateMachine.cast({__MODULE__, remote_node}, {:untrack, rpid})
    send(rpid, {:swarm, :die})
    GenStateMachine.cast({__MODULE__, remote_node}, {:track, rname, lpid, lmeta})
    state
  end

  # A new node has been added to the cluster, we need to update the hash ring and handle shifting
  # processes to new nodes based on the new topology.
  defp nodeup(%TrackerState{nodes: nodes, ring: ring} = state, node) do
    cond do
      node == Node.self ->
        new_ring = ring
        |> HashRing.remove_node(state.self)
        |> HashRing.add_node(node)
        info "node name changed from #{state.self} to #{node}"
        {:ok, %{state | self: node, ring: new_ring}}
      Enum.member?(nodes, node) ->
        {:ok, state}
      ignore_node?(node) ->
        {:ok, state}
      :else ->
        case :rpc.call(node, :application, :ensure_all_started, [:swarm]) do
          {:ok, _} ->
            info "nodeup #{node}"
            new_state = %{state | nodes: [node|nodes], ring: HashRing.add_node(ring, node)}
            {:ok, new_state, {:topology_change, {:nodeup, node}}}
          other ->
            warn "nodeup for #{node} was ignored because swarm failed to start: #{inspect other}"
            {:ok, state}
        end
    end
  end

  # A remote node went down, we need to update the hash ring and handle restarting/shifting processes
  # as needed based on the new topology
  defp nodedown(%TrackerState{nodes: nodes, ring: ring} = state, node) do
    cond do
      Enum.member?(nodes, node) ->
        info "nodedown #{node}"
        ring = HashRing.remove_node(ring, node)
        pending_reqs = Enum.filter(state.pending_sync_reqs, fn ^node -> false; _ -> true end)
        new_state = %{state | nodes: nodes -- [node], ring: ring, pending_sync_reqs: pending_reqs}
        {:ok, new_state, {:topology_change, {:nodedown, node}}}
      :else ->
        {:ok, state}
    end
  end

end
