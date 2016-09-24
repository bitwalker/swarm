defmodule Swarm.Tracker do
  import Swarm.Logger
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

  def start_link() do
    :proc_lib.start_link(__MODULE__, :init, [self()], :infinity, [:link])
  end

  # When the tracker starts, it does the following:
  #    - Creates the ETS table for registry objects
  #    - Registers itself as Swarm.Tracker
  #    - Traps exits so we know when we need to shutdown, or when started
  #      processes exit
  #    - Monitors node up/down events so we can keep the hash ring up to date
  #    - Collects the initial nodelist, and builds the hash ring and tracker state
  #    - Tells the parent process that we've started
  def init(parent) do
    # start ETS table for registry
    :ets.new(:swarm_registry, [
          :set,
          :named_table,
          :public,
          keypos: 2,
          read_concurrency: true,
          write_concurrency: true])
    # register as Swarm.Tracker
    Process.register(self(), __MODULE__)
    # Trap exits
    Process.flag(:trap_exit, true)
    # Tell the supervisor we've started
    :proc_lib.init_ack(parent, {:ok, self()})
    # Start monitoring nodes
    :ok = :net_kernel.monitor_nodes(true, [])
    debug "[tracker] started"
    # Before we can be considered "up", we must sync with
    # some other node in the cluster, if they exist, otherwise
    # we seed our own ITC and start tracking
    debug = :sys.debug_options([])
    # wait for node list to populate
    nodelist = Enum.uniq(wait_for_cluster([]) ++ Node.list)
    ring = Enum.reduce(nodelist, Ring.new(Node.self), fn n, r ->
      Ring.add_node(r, n)
    end)
    state = %TrackerState{nodes: nodelist, ring: ring}
    # join cluster of found nodes
    try do
      join_cluster(state, parent, debug)
    catch
      kind, err ->
        IO.puts Exception.format(kind, err, System.stacktrace)
        IO.puts "Swarm.Tracker terminating abnormally.."
        exit({kind, err})
    end
  end

  # This initial wait loop is just to ensure we give enough time for
  # nodes in the cluster to find and connect to each other before we
  # attempt to join the cluster.
  # The jitter introduced is simply to prevent nodes from starting at the
  # exact same time and racing.
  defp wait_for_cluster(nodes) do
    jitter = :rand.uniform(1_000)
    receive do
      {:nodeup, node} ->
        wait_for_cluster([node|nodes])
      {:nodedown, node} ->
        wait_for_cluster(nodes -- [node])
    after
      5_000 + jitter ->
        nodes
    end
  end

  # This is where we kick off joining and synchronizing with the cluster
  # If there are no known remote nodes, then we become the "seed" node of
  # the cluster, in the sense that our clock will be the initial version
  # of the clock which all other nodes will fork from
  defp join_cluster(%TrackerState{nodes: []} = state, parent, debug) do
    debug "[tracker] joining cluster.."
    debug "[tracker] no connected nodes, proceeding without sync"
    # If no other nodes are connected, start anti-entropy
    # and seed the clock
    :timer.send_after(5 * 60_000, self(), :anti_entropy)
    loop(%{state | clock: ITC.seed()}, parent, debug)
  end
  defp join_cluster(%TrackerState{nodes: nodelist} = state, parent, debug) do
    debug "[tracker] joining cluster.."
    debug "[tracker] found connected nodes: #{inspect nodelist}"
    # Connect to a random node and sync registries,
    # start anti-entropy, and start loop with forked clock of
    # remote node
    sync_node = Enum.random(nodelist)
    debug "[tracker] selected sync node: #{sync_node}"
    # Send sync request
    GenServer.cast({__MODULE__, sync_node}, {:sync, self()})
    # Wait until we receive the sync response before proceeding
    state = begin_sync(sync_node, state)
    :timer.send_after(60_000, self(), :anti_entropy)
    debug "[tracker] started anti-entropy"
    loop(state, parent, debug)
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
  defp begin_sync(sync_node, %TrackerState{nodes: nodes} = state, pending_requests \\ []) do
    receive do
      # We want to handle nodeup/down events while syncing so that if we need to select
      # a new node, we can do so.
      {:nodeup, node} ->
        debug "[tracker] node up #{node}"
        begin_sync(sync_node, %{state | nodes: [node|nodes]}, pending_requests)
      # If our target node goes down, we need to select a new target or become the seed node
      {:nodedown, ^sync_node} ->
        debug "[tracker] the selected sync node #{sync_node} went down, selecting new node"
        case nodes -- [sync_node] do
          [] ->
            # there are no other nodes to select, we'll be the seed
            debug "[tracker] no other available nodes, becoming seed node"
            %{state | nodes: [], clock: ITC.seed()}
          nodes ->
            new_sync_node = Enum.random(nodes)
            debug "[tracker] selected sync node: #{new_sync_node}"
            # Send sync request
            GenServer.cast({__MODULE__, new_sync_node}, {:sync, self()})
            begin_sync(new_sync_node, %{state | nodes: nodes}, pending_requests)
        end
      # Keep the hash ring up to date if other nodes go down
      {:nodedown, node} ->
        debug "[tracker] node down #{node}"
        begin_sync(sync_node, %{state | nodes: nodes -- [node]}, pending_requests)
      # Receive a copy of the registry from our sync target
      {:sync_recv, from, clock, registry} ->
        debug "[tracker] received sync response, loading registry.."
        # let remote node know we've got the registry
        send(from, {:sync_ack, Node.self})
        # load target registry
        for entry(name: name, pid: pid, meta: meta, clock: clock) <- registry do
          ref = Process.monitor(pid)
          :ets.insert(:swarm_registry, entry(name: name, pid: pid, ref: ref, meta: meta, clock: clock))
        end
        # update our local clock to match remote clock
        state = %{state | clock: clock}
        debug "[tracker] finished sync and sent acknowledgement to #{node(from)}"
        # clear any pending sync requests to prevent blocking
        resolve_pending_sync_requests(state, pending_requests)
      # Something weird happened during sync, so try a different node,
      # with this implementation, we *could* end up selecting the same node
      # again, but that's fine as this is effectively a retry
      {:sync_err, _from} when length(nodes) > 0 ->
        debug "[tracker] sync error, choosing a new node to sync with"
        # we need to choose a different node to sync with and try again
        sync_node = Enum.random(nodes)
        GenServer.cast({__MODULE__, sync_node}, {:sync, self()})
        begin_sync(sync_node, state, pending_requests)
      # Something went wrong during sync, but there are no other nodes to sync with,
      # not even the original sync node (which probably implies it shutdown or crashed),
      # so we're the sync node now
      {:sync_err, _from} ->
        debug "[tracker] sync error, and no other available sync targets, becoming seed node"
        resolve_pending_sync_requests(%{state | clock: ITC.seed()}, pending_requests)
      # Incoming sync request from our target node, in other words, A is trying to sync with B,
      # and B is trying to sync with A - we need to break the deadlock
      {:'$gen_cast', {:sync, from}} when node(from) == sync_node ->
        debug "[tracker] received sync request during initial sync"
        cond do
          # Well shit, we can't pass the request off to another node, so let's roll a die
          # to choose who will sync with who.
          length(nodes) == 1 ->
            # 20d, I mean why not?
            die = :rand.uniform(20)
            debug "[tracker] there is a tie between syncing nodes, breaking with die roll (#{die}).."
            send(from, {:sync_break_tie, self(), die})
            receive do
              {:nodedown, ^sync_node} ->
                # welp, guess we'll try a new node
                case nodes -- [sync_node] do
                  [] ->
                    debug "[tracker] sync with #{sync_node} cancelled: nodedown, no other nodes available, so becoming seed"
                    resolve_pending_sync_requests(%{state | nodes: []}, pending_requests)
                  nodes ->
                    debug "[tracker] sync with #{sync_node} cancelled: nodedown, retrying with a new node"
                    sync_node = Enum.random(nodes)
                    GenServer.cast({__MODULE__, sync_node}, {:sync, self()})
                    begin_sync(sync_node, %{state | nodes: nodes}, pending_requests)
                end
              {:sync_break_tie, from, die2} when die2 > die or (die2 == die and node(from) > node(self)) ->
                debug "[tracker] #{node(from)} won the die roll (#{die2} vs #{die}), waiting for payload.."
                # The other node won the die roll, either by a greater die roll, or the absolute
                # tie breaker of node ordering
                begin_sync(sync_node, state, pending_requests)
              {:sync_break_tie, _, die2} ->
                debug "[tracker] we won the die roll (#{die} vs #{die2}), sending payload.."
                # This is the new seed node
                {clock, rclock} = ITC.fork(ITC.seed())
                send(from, {:sync_recv, self(), rclock, []})
                receive do
                  {:nodedown, ^sync_node} ->
                    # we sent the registry, but the node went down before the ack, just continue..
                    :ok
                  {:sync_ack, _} ->
                    debug "[tracker] sync request for #{node(from)} completed"
                end
                # clear any pending sync requests to prevent blocking
                resolve_pending_sync_requests(%{state | clock: clock}, pending_requests)
            end
          :else ->
            debug "[tracker] rejecting sync request since we're still in initial sync"
            # there are other nodes, tell the requesting node to choose another node to sync with
            # and then we'll sync with them
            send(from, {:sync_err, self()})
            begin_sync(sync_node, state, pending_requests)
        end
      {:'$gen_cast', {:sync, from}} ->
        debug "[tracker] pending sync request from #{node(from)}"
        begin_sync(sync_node, state, [from|pending_requests])
    after 30_000 ->
        debug "[tracker] failed to sync with #{sync_node} after 30s, selecting a new node and retrying"
        case nodes -- [sync_node] do
          [] ->
            debug "[tracker] no other nodes to sync with, becoming seed node"
            resolve_pending_sync_requests(%{state | clock: ITC.seed()}, pending_requests)
          nodes ->
            new_sync_node = Enum.random(nodes)
            debug "[tracker] selected new sync node #{new_sync_node}"
            GenServer.cast({__MODULE__, new_sync_node}, {:sync, self()})
            begin_sync(new_sync_node, nodes, pending_requests)
        end
    end
  end

  @spec resolve_pending_sync_requests(TrackerState.t, [pid()]) :: TrackerState.t
  defp resolve_pending_sync_requests(%TrackerState{} = state, pending) do
    # clear any pending sync requests to prevent blocking
    registry = :ets.tab2list(:swarm_registry)
    new_state = Enum.reduce(pending, state, fn pid, acc ->
      pending_node = node(pid)
      debug "[tracker] clearing pending sync request for #{pending_node}"
      {lclock, rclock} = ITC.fork(acc.clock)
      send(pid, {:sync_recv, self(), rclock, registry})
      receive do
        {:nodedown, ^pending_node} ->
          debug "[tracker] sync request from #{pending_node} cancelled: nodedown"
          %{state | nodes: state.nodes -- [pending_node]}
        {:sync_ack, ^pid} ->
          debug "[tracker] sync request for #{node(pid)} completed"
          %{acc | clock: lclock}
      after
        5_000 ->
          warn "[tracker] did not receive acknowledgement for sync response from #{node(pid)}"
          acc
      end
    end)
    debug "[tracker] pending sync requests cleared"
    new_state
  end

  defp handle_anti_entropy(%TrackerState{nodes: []} = state) do
    :timer.send_after(5 * 60_000, self(), :anti_entropy)
    {:ok, state}
  end
  defp handle_anti_entropy(%TrackerState{nodes: nodes} = state) do
    sync_node = Enum.random(nodes)
    debug "[tracker] [anti-entropy] syncing with #{sync_node}"
    GenServer.cast({__MODULE__, sync_node}, {:sync, self()})
    {elapsed, state} = :timer.tc(fn -> complete_anti_entropy(sync_node, state) end)
    debug "[tracker] [anti-entropy] completed in: #{Float.round(elapsed/1_000, 3)}ms"
    :timer.send_after(5 * 60_000, self(), :anti_entropy)
    {:ok, state}
  end

  defp complete_anti_entropy(sync_node, state) do
    receive do
      {:nodedown, ^sync_node} ->
        warn "[tracker] [anti-entropy] sync with #{sync_node} failed: nodedown, aborting this pass"
        state
      {:sync_recv, from, _rclock, registry} ->
        debug "[tracker] [anti-entropy] received registry from #{sync_node}, checking.."
        # let remote node know we've got the registry
        send(from, {:sync_ack, Node.self})
        # map over the registry and check that all local entries are correct
        Enum.reduce(registry, state, fn entry(name: rname, pid: rpid, meta: rmeta, clock: rclock) = rreg, %TrackerState{clock: clock} = state ->
          case :ets.lookup(:swarm_registry, rname) do
            [] ->
              # missing local registration
              cond do
                ITC.leq(rclock, clock) ->
                  # our clock is ahead, so we'll do nothing
                  debug "[tracker] [anti-entropy] local is missing #{inspect rname}, but local clock is dominant, ignoring"
                  state
                :else ->
                  # our clock is behind, so add the registration
                  debug "[tracker] [anti-entropy] local is missing #{inspect rname}, adding.."
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
                  warn "[tracker] [anti-entropy] local and remote metadata for #{inspect rname} conflicts, keeping local for now"
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
                  debug "[tracker] [anti-entropy] remote view of #{inspect rname} is outdated, resolving.."
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
                      debug "[tracker] [anti-entropy] remote and local view of #{inspect rname} conflict, but remote is correct, resolving.."
                      resolve_incorrect_local_reg(sync_node, lreg, rreg, state)
                    target_node == lpid_node and rpid_node != lpid_node ->
                      debug "[tracker] [anti-entropy] remote and local view of #{inspect rname} conflict, but local is correct, resolving.."
                      resolve_incorrect_remote_reg(sync_node, lreg, rreg, state)
                    lpid_node == rpid_node && rpid > lpid ->
                      debug "[tracker] [anti-entropy] remote and local view of #{inspect rname} conflict, " <>
                        "but the remote view is more recent, resolving.."
                      resolve_incorrect_local_reg(sync_node, lreg, rreg, state)
                    lpid_node == rpid_node && lpid > rpid ->
                      debug "[tracker] [anti-entropy] remote and local view of #{inspect rname} conflict, " <>
                        "but the local view is more recent, resolving.."
                      resolve_incorrect_remote_reg(sync_node, lreg, rreg, state)
                    :else ->
                      # the registrations are inconsistent with each other, and cannot be resolved
                      warn "[tracker] [anti-entropy] remote and local view of #{inspect rname} conflict, " <>
                        "but cannot be resolved"
                      state
                  end
              end
          end
        end)
    end
  end

  # Used during anti-entropy checks to remove local registrations and replace them with the remote version
  defp resolve_incorrect_local_reg(_remote_node, entry(pid: lpid) = lreg, entry(name: rname, pid: rpid, meta: rmeta, clock: rclock), state) do
    # the remote registration is correct
    {:ok, state} = remove_registration(state, lreg)
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
  defp handle_nodeup(node, %TrackerState{nodes: nodelist, ring: ring} = state) do
    case :rpc.call(node, :application, :ensure_all_started, [:swarm]) do
      {:ok, _} ->
        debug "[tracker] nodeup #{node}"
        ring = Ring.add_node(ring, node)
        handle_topology_change({:nodeup, node}, %{state | nodes: [node|nodelist], ring: ring})
      _ ->
        {:ok, state}
    end
  end

  # A remote node went down, we need to update the hash ring and handle restarting/shifting processes
  # as needed based on the new topology
  defp handle_nodedown(node, %TrackerState{nodes: nodelist, ring: ring} = state) do
    debug "[tracker] nodedown #{node}"
    ring = Ring.remove_node(ring, node)
    handle_topology_change({:nodedown, node}, %{state | nodes: nodelist -- [node], ring: ring})
  end

  # This is the primary receive loop. It's complexity is in large part due to the fact that we
  # need to prioritize certain messages as they have implications on how things are tracked, as
  # well as the need to ensure that one or more instances of the tracker can't deadlock each other
  # when they need to coordinate. The implementation of these is almost entirely implemented via
  # handle_* functions to keep the loop clearly structured.
  defp loop(state, parent, debug) do
    receive do
      # System messages take precedence, as does the parent process exiting
      {:system, from, request} ->
        debug "[tracker] sys: #{inspect request}"
        :sys.handle_system_msg(request, from, parent, __MODULE__, debug, state)
      {:EXIT, ^parent, reason} ->
        debug "[tracker] exiting: #{inspect reason}"
        exit(reason)
      {:EXIT, _child, _reason} ->
        # a process started by the tracker died, ignore
        loop(state, parent, debug)
      {:nodeup, node} ->
        {:ok, state} = handle_nodeup(node, state)
        loop(state, parent, debug)
      {:nodedown, node} ->
        {:ok, state} = handle_nodedown(node, state)
        loop(state, parent, debug)
      :anti_entropy ->
        {:ok, state} = handle_anti_entropy(state)
        loop(state, parent, debug)
      # Received a handoff request from a node
      {from, {:handoff, name, m, f, a, handoff_state, rclock}} ->
        case handle_handoff(from, name, m, f, a, handoff_state, rclock, state) do
          {:ok, state} ->
            loop(state, parent, debug)
          {{:error, err}, state} ->
            warn "[tracker] handoff failed for #{inspect name}: #{inspect err}"
            loop(state, parent, debug)
        end
        loop(state, parent, debug)
      # A remote registration failed due to nodedown during the call
      {:retry, {:track, name, m, f, a, from}} ->
        {:ok, state} = handle_retry({:track, name, m, f, a}, from, state)
        loop(state, parent, debug)
      # A change event received from another node
      {:event, from, rclock, event} ->
        {:ok, state} = handle_event(event, from, rclock, state)
        loop(state, parent, debug)
      # A change event received locally
      {:'$gen_call', from, call} ->
        {:ok, state} = do_handle_call(call, from, state)
        loop(state, parent, debug)
      {:'$gen_cast', cast} ->
        {:noreply, state} = handle_cast(cast, state)
        loop(state, parent, debug)
      {:DOWN, ref, _type, pid, info} ->
        {:ok, state} = handle_monitor(ref, pid, info, state)
        loop(state, parent, debug)
      msg ->
        debug "[tracker] unexpected message: #{inspect msg}"
        loop(state, parent, debug)
    end
  end

  # This is the callback for when a process is being handed off from a remote node to this node.
  defp handle_handoff(_from, name, m, f, a, handoff_state, _rclock, %TrackerState{clock: clock} = state) do
    try do
      {:ok, pid} = apply(m, f, a)
      GenServer.cast(pid, {:swarm, :end_handoff, handoff_state})
      ref = Process.monitor(pid)
      clock = ITC.event(clock)
      meta = %{mfa: {m,f,a}}
      :ets.insert(:swarm_registry, entry(name: name, pid: pid, ref: ref, meta: meta, clock: ITC.peek(clock)))
      broadcast_event(state.nodes, ITC.peek(clock), {:track, name, pid, meta})
      {:ok, %{state | clock: clock}}
    catch
      kind, err ->
        IO.puts Exception.normalize(kind, err, System.stacktrace)
        {{:error, err}, state}
    end
  end

  # This is the callback for when a nodeup/down event occurs after the tracker has entered
  # the main receive loop. Topology changes are handled a bit differently during startup.
  defp handle_topology_change({type, remote_node}, %TrackerState{} = state) do
    debug "[tracker] topology change (#{type} for #{remote_node})"
    current_node = Node.self
    clock = :ets.foldl(fn
      entry(name: name, pid: pid, meta: %{mfa: {m,f,a}}) = obj, lclock when node(pid) == current_node ->
        case Ring.key_to_node(state.ring, name) do
          ^current_node ->
            # This process is correct
            lclock
          other_node ->
            debug "[tracker] #{inspect pid} belongs on #{other_node}"
            # This process needs to be moved to the new node
            try do
              case GenServer.call(pid, {:swarm, :begin_handoff}) do
                :ignore ->
                  debug "[tracker] #{inspect pid} has requested to be ignored"
                  lclock
                {:resume, handoff_state} ->
                  debug "[tracker] #{inspect pid} has requested to be resumed"
                  {:noreply, state} = remove_registration(%{state | clock: lclock}, obj)
                  send(pid, {:swarm, :die})
                  debug "[tracker] sending handoff to #{remote_node}"
                  send({__MODULE__, remote_node}, {self(), {:handoff, name, m, f, a, handoff_state, ITC.peek(state.clock)}})
                  state.clock
                :restart ->
                  debug "[tracker] #{inspect pid} has requested to be restarted"
                  {:noreply, state} = remove_registration(%{state | clock: lclock}, obj)
                  send(pid, {:swarm, :die})
                  {:reply, _, state} = handle_call({:track, name, m, f, a}, nil, %{state | clock: lclock})
                  state.clock
              end
            catch
              _, err ->
                warn "[tracker] handoff failed for #{inspect pid}: #{inspect err}"
                lclock
            end
        end
      entry(name: name, pid: pid), lclock ->
        debug "[tracker] doing nothing for #{inspect name}, it is owned by #{node(pid)}"
        lclock
    end, state.clock, :swarm_registry)
    debug "[tracker] topology change complete"
    {:ok, %{state | clock: clock}}
  end

  # This is the callback for tracker events which are being replicated from other nodes in the cluster
  defp handle_event({:track, name, pid, meta}, _from, rclock, %TrackerState{clock: clock} = state) do
    debug "[tracker] event: track #{inspect {name, pid, meta}}"
    cond do
      ITC.leq(clock, rclock) ->
        ref = Process.monitor(pid)
        :ets.insert(:swarm_registry, entry(name: name, pid: pid, ref: ref, meta: meta, clock: rclock))
        {:ok, %{state | clock: ITC.event(clock)}}
      :else ->
        warn "[tracker] received track event for #{inspect name}, but local clock conflicts with remote clock, event unhandled"
        # TODO: Handle conflict?
        {:ok, state}
    end

  end
  defp handle_event({:untrack, pid}, _from, rclock, %TrackerState{clock: clock} = state) do
    debug "[tracker] event: untrack #{inspect pid}"
    cond do
      ITC.leq(clock, rclock) ->
        case Registry.get_by_pid(pid) do
          :undefined ->
            {:ok, state}
          entry(ref: ref, clock: lclock) = obj ->
            cond do
              ITC.leq(lclock, rclock) ->
                # registration came before unregister, so remove the registration
                Process.demonitor(ref, [:flush])
                :ets.delete_object(:swarm_registry, obj)
                {:ok, %{state | clock: ITC.event(clock)}}
              :else ->
                # registration is newer than de-registration, ignore msg
                debug "[tracker] untrack is causally dominated by track for #{inspect pid}, ignoring.."
                {:ok, state}
            end
        end
      :else ->
        warn "[tracker] received untrack event, but local clock conflicts with remote clock, event unhandled"
        # Handle conflict?
        {:ok, state}
    end
  end
  defp handle_event({:add_meta, key, value, pid}, _from, rclock, %TrackerState{clock: clock} = state) do
    debug "[tracker] event: add_meta #{inspect {key, value}} to #{inspect pid}"
    case Registry.get_by_pid(pid) do
      :undefined ->
        {:ok, state}
      entry(name: name, meta: old_meta, clock: lclock) ->
        cond do
          ITC.leq(lclock, rclock) ->
            new_meta = Map.put(old_meta, key, value)
            :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, rclock}])
            {:ok, %{state | clock: ITC.event(clock)}}
          :else ->
            # we're going to take the last-writer wins approach for resolution for now
            new_meta = Map.merge(old_meta, %{key => value})
            # we're going to keep our local clock though and re-broadcast the update to ensure we converge
            clock = ITC.event(clock)
            :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, ITC.peek(clock)}])
            debug "[tracker] conflicting meta for #{inspect name}, updating and notifying other nodes"
            broadcast_event(state.nodes, ITC.peek(clock), {:update_meta, new_meta, pid})
            {:ok, %{state | clock: clock}}
        end
    end
  end
  defp handle_event({:update_meta, new_meta, pid}, _from, rclock, %TrackerState{clock: clock} = state) do
    debug "[tracker] event: update_meta #{inspect new_meta} for #{inspect pid}"
    case Registry.get_by_pid(pid) do
      :undefined ->
        {:ok, state}
      entry(name: name, meta: old_meta, clock: lclock) ->
        cond do
          ITC.leq(lclock, rclock) ->
            :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, rclock}])
            {:ok, %{state | clock: ITC.event(clock)}}
          :else ->
            # we're going to take the last-writer wins approach for resolution for now
            new_meta = Map.merge(old_meta, new_meta)
            # we're going to keep our local clock though and re-broadcast the update to ensure we converge
            clock = ITC.event(clock)
            :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, ITC.peek(clock)}])
            debug "[tracker] conflicting meta for #{inspect name}, updating and notifying other nodes"
            broadcast_event(state.nodes, ITC.peek(clock), {:update_meta, new_meta, pid})
            {:ok, %{state | clock: clock}}
        end
    end
  end
  defp handle_event({:remove_meta, key, pid}, _from, rclock, %TrackerState{clock: clock} = state) do
    debug "[tracker] event: remove_meta #{inspect key} from #{inspect pid}"
    case Registry.get_by_pid(pid) do
      :undefined ->
        {:ok, state}
      entry(name: name, meta: meta, clock: lclock) ->
        cond do
          ITC.leq(lclock, rclock) ->
            new_meta = Map.drop(meta, [key])
            :ets.update_element(:swarm_registry, name, [{entry(:meta)+1, new_meta}, {entry(:clock)+1, rclock}])
            {:ok, %{state | clock: ITC.event(clock)}}
          :else ->
            warn "[tracker] received remove_meta event, but local clock conflicts with remote clock, event unhandled"
            # Handle conflict?
            {:ok, state}
        end
    end
  end

  # This is the handler for local operations on the tracker which require a response.
  defp handle_call({:track, name, pid, meta}, _from, %TrackerState{} = state) do
    debug "[tracker] call: track #{inspect {name, pid, meta}}"
    add_registration(state, name, pid, meta)
  end
  defp handle_call({:track, name, m, f, a}, from, %TrackerState{ring: ring} = state) do
    debug "[tracker] call: track #{inspect {name, m, f, a}}"
    current_node = Node.self
    case Ring.key_to_node(ring, name) do
      ^current_node ->
        debug "[tracker] starting #{inspect {m,f,a}} on #{current_node}"
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
        debug "[tracker] starting #{inspect {m,f,a}} on #{remote_node}"
        Task.start(fn ->
          try do
            reply = GenServer.call({__MODULE__, remote_node}, {:track, name, m, f, a}, :infinity)
            GenServer.reply(from, reply)
          catch
            _, err ->
              warn "[tracker] failed to start #{inspect name} on #{remote_node}: #{inspect err}, retrying operation.."
              send(__MODULE__, {:retry, {:track, name, m, f, a, from}})
          end
        end)
        {:noreply, state}
    end
  end

  # This is the handler for local operations on the tracker which are asynchronous
  defp handle_cast({:untrack, pid}, %TrackerState{} = state) do
    debug "[tracker] call: untrack #{inspect pid}"
    remove_registration_by_pid(state, pid)
  end
  defp handle_cast({:add_meta, key, value, pid}, %TrackerState{} = state) do
    debug "[tracker] call: add_meta #{inspect {key, value}} to #{inspect pid}"
    add_meta_by_pid(state, {key, value}, pid)
  end
  defp handle_cast({:remove_meta, key, pid}, %TrackerState{} = state) do
    remove_meta_by_pid(state, key, pid)
  end
  defp handle_cast({:sync, from}, %TrackerState{clock: clock} = state) do
    debug "[tracker] received sync request from #{node(from)}"
    {lclock, rclock} = ITC.fork(clock)
    send(from, {:sync_recv, self(), rclock, :ets.tab2list(:swarm_registry)})
    wait_for_sync_ack(node(from))
    {:ok, %{state | clock: lclock}}
  end

  # This is only ever called if a registration needs to be sent to a remote node
  # and that node went down in the middle of the call to it's Swarm process.
  # We need to process the nodeup/down events by re-entering the receive loop first,
  # so we send ourselves a message to retry, this is the handler for that message
  defp handle_retry({:track, _, _, _, _} = event, from, state) do
    case handle_call(event, from, state) do
      {:reply, reply, state} ->
        GenServer.reply(from, reply)
        {:ok, state}
        {:noreply, state}
        {:ok, state}
    end
  end

  # Called when a pid dies, and the monitor is triggered
  defp handle_monitor(ref, pid, :noconnection, %TrackerState{} = state) do
    # lost connection to the node this pid is running on, check if we should restart it
    debug "[tracker] lost connection to pid (#{inspect pid}), checking to see if we should revive it"
    case Registry.get_by_ref(ref) do
      :undefined ->
        debug "[tracker] could not find pid #{inspect pid}"
        {:ok, state}
      entry(name: name, pid: ^pid, ref: ^ref, meta: %{mfa: {m,f,a}}) = obj ->
        current_node = Node.self
        # this event may have occurred before we get a nodedown event, so
        # for the purposes of this handler, preemptively remove the node from the
        # ring when calculating the new node
        ring  = Ring.remove_node(state.ring, node(pid))
        state = %{state | ring: ring}
        case Ring.key_to_node(ring, name) do
          ^current_node ->
            debug "[tracker] restarting pid #{inspect pid} on #{current_node}"
            {:noreply, state} = remove_registration(state, obj)
            {:reply, _, state} = handle_call({:track, name, m, f, a}, nil, state)
            {:ok, state}
          other_node ->
            debug "[tracker] pid belongs on #{other_node}"
            {:noreply, state} = remove_registration(state, obj)
            {:ok, state}
        end
      entry(pid: pid, ref: ^ref, meta: meta) = obj ->
        debug "[tracker] nothing to do for pid #{inspect pid} (meta: #{inspect meta})"
        {:noreply, state} = remove_registration(state, obj)
        {:ok, state}
    end
  end
  defp handle_monitor(ref, pid, reason, %TrackerState{} = state) do
    debug "[tracker] pid (#{inspect pid}) down: #{inspect reason}"
    case Registry.get_by_ref(ref) do
      :undefined ->
        {:ok, state}
      entry(ref: ^ref) = obj ->
        {:noreply, state} = remove_registration(state, obj)
        {:ok, state}
    end
  end

  # Sys module callbacks

  # Handle resuming this process after it's suspended by :sys
  # We're making a bit of an assumption here that it won't be suspended
  # prior to entering the receive loop. This is something we (probably) could
  # fix by storing the current phase of the startup the process is in, but I'm not sure.
  def system_continue(parent, debug, state) do
    loop(state, parent, debug)
  end

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
  def system_get_state(state) do
    {:ok, state}
  end

  # Called when someone asks to replace the current process state
  # Required, but you really really shouldn't do this.
  def system_replace_state(state_fun, state) do
    new_state = state_fun.(state)
    {:ok, new_state, new_state}
  end

  # Called when the system is upgrading this process
  def system_code_change(misc, _module, _old, _extra) do
    {:ok, misc}
  end

  # Used for tracing with sys:handle_debug
  #defp write_debug(_dev, event, name) do
  #  Swarm.Logger.debug("[tracker] #{inspect name}: #{inspect event}")
  #end

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

  # Wrap handle_call to behave more like a GenServer would
  defp do_handle_call(call, from, state) do
    case handle_call(call, from, state) do
      {:reply, reply, state} ->
        GenServer.reply(from, reply)
        {:ok, state}
      {:noreply, state} ->
        {:ok, state}
    end
  end

  # Block until an acknowledgement is received from the remote node,
  # or until the remote node goes down
  defp wait_for_sync_ack(remote_node) do
    receive do
      {:nodedown, ^remote_node} ->
        warn "[tracker] sync request from #{remote_node} cancelled, node went down"
      {:sync_ack, ^remote_node} ->
        debug "[tracker] sync with #{remote_node} complete"
    after 1_000 ->
        debug "[tracker] waiting for sync acknolwedgment.."
        wait_for_sync_ack(remote_node)
    end
  end

end
