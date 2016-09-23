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
    :ok = :net_kernel.monitor_nodes(true, [node_type: :all])
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

  defp wait_for_cluster(nodes) do
    jitter = :rand.uniform(1_000)
    receive do
      {:nodeup, node, _info} ->
        wait_for_cluster([node|nodes])
      {:nodeup, node} ->
        wait_for_cluster([node|nodes])
      {:nodedown, node, _info} ->
        wait_for_cluster(nodes -- [node])
      {:nodedown, node} ->
        wait_for_cluster(nodes -- [node])
    after
      5_000 + jitter ->
        nodes
    end
  end

  defp join_cluster(%TrackerState{nodes: []} = state, parent, debug) do
    debug "[tracker] joining cluster.."
    debug "[tracker] no connected nodes, proceeding without sync"
    # If no other nodes are connected, start anti-entropy
    # and seed the clock
    :timer.send_after(60_000, self(), :anti_entropy)
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

  defp begin_sync(sync_node, %TrackerState{nodes: nodes} = state, pending_requests \\ []) do
    receive do
      # We want to handle nodeup/down events while syncing so that if we need to select
      # a new node, we can do so.
      {:nodeup, node, _info} ->
        debug "[tracker] node up #{node}"
        begin_sync(sync_node, %{state | nodes: [node|nodes]}, pending_requests)
      {:nodeup, node} ->
        debug "[tracker] node up #{node}"
        begin_sync(sync_node, %{state | nodes: [node|nodes]}, pending_requests)
      {:nodedown, ^sync_node, _info} ->
        debug "[tracker] the selected sync node #{sync_node} went down, selecting new node"
        nodes = nodes -- [sync_node]
        new_sync_node = Enum.random(nodes)
        debug "[tracker] selected sync node: #{new_sync_node}"
        # Send sync request
        GenServer.cast({__MODULE__, new_sync_node}, {:sync, self()})
        begin_sync(new_sync_node, %{state | nodes: nodes}, pending_requests)
      {:nodedown, ^sync_node} ->
        debug "[tracker] the selected sync node #{sync_node} went down, selecting new node"
        nodes = nodes -- [sync_node]
        new_sync_node = Enum.random(nodes)
        # Send sync request
        GenServer.cast({__MODULE__, new_sync_node}, {:sync, self()})
        begin_sync(new_sync_node, %{state | nodes: nodes}, pending_requests)
      {:nodedown, node, _info} ->
        debug "[tracker] node down #{node}"
        begin_sync(sync_node, %{state | nodes: nodes -- [node]}, pending_requests)
      {:nodedown, node} ->
        debug "[tracker] node down #{node}"
        begin_sync(sync_node, %{state | nodes: nodes -- [node]}, pending_requests)
      {:sync_recv, from, clock, registry} ->
        # sync with node
        debug "[tracker] received sync response, loading registry.."
        for entry(name: name, pid: pid, meta: meta, clock: clock) <- registry do
          ref = Process.monitor(pid)
          :ets.insert(:swarm_registry, entry(name: name, pid: pid, ref: ref, meta: meta, clock: clock))
        end
        send(from, {:sync_ack, Node.self})
        state = %{state | clock: clock}
        debug "[tracker] finished sync and sent acknowledgement to #{node(from)}"
        # clear any pending sync requests to prevent blocking
        state = Enum.reduce(pending_requests, state, fn pid, acc ->
          debug "[tracker] clearing pending sync request for #{node(pid)}"
          {lclock, rclock} = ITC.fork(acc.clock)
          send(pid, {:sync_recv, self(), rclock, registry})
          receive do
            {:sync_ack, ^pid} ->
              debug "[tracker] sync request for #{node(pid)} completed"
          after
            5_000 ->
              warn "[tracker] did not receive acknowledgement for sync response from #{node(pid)}"
          end
          %{acc | :clock => lclock}
        end)
        debug "[tracker] pending sync requests cleared"
        state
      {:sync_err, _from} ->
        debug "[tracker] sync error, choosing a new node to sync with"
        # we need to choose a different node to sync with and try again
        sync_node = Enum.random(nodes)
        GenServer.cast({__MODULE__, sync_node}, {:sync, self()})
        begin_sync(sync_node, state, pending_requests)
      {:'$gen_cast', {:sync, from}} when node(from) == sync_node ->
        debug "[tracker] received sync request during initial sync"
        # the two nodes are trying to sync with each other
        cond do
          length(nodes) == 1 ->
            # roll die to choose which node becomes sync node
            die = :rand.uniform(20)
            debug "[tracker] there is a tie between syncing nodes, breaking with die roll (#{die}).."
            send(from, {:sync_break_tie, self(), die})
            receive do
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
                  {:sync_ack, _} ->
                    debug "[tracker] sync request for #{node(from)} completed"
                end
                # clear any pending sync requests to prevent blocking
                Enum.reduce(pending_requests, %{state | clock: clock}, fn pid, acc ->
                  debug "[tracker] clearing pending sync request for #{node(pid)}"
                  {lclock, rclock} = ITC.fork(acc.clock)
                  send(pid, {:sync_recv, self(), rclock, []})
                  receive do
                    {:sync_ack, ^pid} ->
                      debug "[tracker] sync request for #{node(pid)} completed"
                      %{acc | clock: lclock}
                  after
                    5_000 ->
                      warn "[tracker] did not receive acknowledgement for sync response from #{node(pid)}"
                      acc
                  end
                end)
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
        new_sync_node = Enum.random(nodes -- [sync_node])
        GenServer.cast({__MODULE__, new_sync_node}, {:sync, self()})
        begin_sync(new_sync_node, state, pending_requests)
    end
  end

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
  defp handle_nodedown(node, %TrackerState{nodes: nodelist, ring: ring} = state) do
    debug "[tracker] nodedown #{node}"
    ring = Ring.remove_node(ring, node)
    handle_topology_change({:nodedown, node}, %{state | nodes: nodelist -- [node], ring: ring})
  end

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
      {:nodeup, node, _info} ->
        {:ok, state} = handle_nodeup(node, state)
        loop(state, parent, debug)
      {:nodeup, node} ->
        {:ok, state} = handle_nodeup(node, state)
        loop(state, parent, debug)
      {:nodedown, node, _info} ->
        {:ok, state} = handle_nodedown(node, state)
        loop(state, parent, debug)
      {:nodedown, node} ->
        {:ok, state} = handle_nodedown(node, state)
        loop(state, parent, debug)
      :anti_entropy ->
        debug "[tracker] performing anti entropy"
        loop(state, parent, debug)
      # Received from another node requesting a sync
      {from, :sync} ->
        {:ok, state} = handle_sync(from, state)
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

  defp wait_for_sync_ack(remote_node) do
    receive do
      {:sync_ack, ^remote_node} ->
        debug "[tracker] sync with #{remote_node} complete"
      after 1_000 ->
        debug "[tracker] waiting for sync acknolwedgment.."
        wait_for_sync_ack(remote_node)
    end
  end

  defp handle_sync(from, %TrackerState{clock: clock} = state) do
    debug "[tracker] received sync request from #{node(from)}"
    {lclock, rclock} = ITC.fork(clock)
    reply = {:ok, self(), rclock, :ets.tab2list(:swarm_registry)}
    send(from, {node(from), Node.self, reply})
    wait_for_sync_ack(node(from))
    {:ok, %{state | clock: lclock}}
  end

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

  # :sys callbacks

  def system_continue(parent, debug, state) do
    loop(state, parent, debug)
  end

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

  def system_get_state(state) do
    {:ok, state}
  end

  def system_replace_state(state_fun, state) do
    new_state = state_fun.(state)
    {:ok, new_state, new_state}
  end

  def system_code_change(misc, _module, _old, _extra) do
    {:ok, misc}
  end

  # Used for tracing with sys:handle_debug
  #defp write_debug(_dev, event, name) do
  #  Swarm.Logger.debug("[tracker] #{inspect name}: #{inspect event}")
  #end

  defp do_handle_call(call, from, state) do
    case handle_call(call, from, state) do
      {:reply, reply, state} ->
        GenServer.reply(from, reply)
        {:ok, state}
      {:noreply, state} ->
        {:ok, state}
    end
  end
end
