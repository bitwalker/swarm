defmodule Swarm.TrackerReplicaEventTests do
  use ExUnit.Case, async: false

  import Swarm.Entry
  alias Swarm.IntervalTreeClock, as: Clock
  alias Swarm.Registry, as: Registry

  @moduletag :capture_log

  setup_all do
    :rand.seed(:exs64)
    Application.ensure_all_started(:swarm)
    {:ok, _} = MyApp.WorkerSup.start_link()
    :ok
  end

  setup do
    :ets.delete_all_objects(:swarm_registry)

    {:ok, pid} = MyApp.WorkerSup.register()
    meta = %{mfa: {MyApp.WorkerSup, :register, []}}
    name = :rand.uniform()
    {lclock, rclock} = Clock.fork(Clock.seed())

    [name: name, pid: pid, meta: meta, lclock: lclock, rclock: rclock]
  end

  test "handle_replica_event :track should add registration", %{
    name: name,
    pid: pid,
    meta: meta,
    rclock: rclock
  } do
    send_replica_event(rclock, {:track, name, pid, meta})

    assert ^pid = Registry.whereis(name)
    assert Enum.member?(Registry.all(), {name, pid})
    assert entry(name: _, pid: ^pid, ref: ref, meta: ^meta, clock: _) = Registry.get_by_name(name)
    assert [entry(name: ^name, pid: _, ref: _, meta: _, clock: _)] = Registry.get_by_pid(pid)

    assert entry(name: _, pid: _, ref: ^ref, meta: _, clock: _) =
             Registry.get_by_pid_and_name(pid, name)

    assert entry(name: _, pid: ^pid, ref: _, meta: _, clock: _) = Registry.get_by_ref(ref)

    assert [entry(name: _, pid: ^pid, ref: _, meta: _, clock: _)] =
             Registry.get_by_meta(:mfa, {MyApp.WorkerSup, :register, []})

    assert [entry(name: _, pid: ^pid, ref: _, meta: _, clock: _)] =
             :ets.lookup(:swarm_registry, name)
  end

  test "handle_replica_event :track with existing registration should ignore the event", %{
    name: name,
    pid: pid,
    meta: meta,
    lclock: lclock,
    rclock: rclock
  } do
    send_replica_event(lclock, {:track, name, pid, meta})

    send_replica_event(rclock, {:track, name, pid, meta})

    assert entry(name: _, pid: ^pid, ref: _, meta: _, clock: _) = Registry.get_by_name(name)
  end

  test "handle_replica_event :track with conflicting metadata and remote clock dominates should update the metadata",
       %{name: name, pid: pid, meta: meta, lclock: lclock, rclock: rclock} do
    send_replica_event(lclock, {:track, name, pid, meta})

    rclock = Clock.event(rclock)
    remote_meta = %{other: "meta"}
    send_replica_event(rclock, {:track, name, pid, remote_meta})

    assert entry(name: _, pid: ^pid, ref: _, meta: ^remote_meta, clock: _) =
             Registry.get_by_name(name)
  end

  test "handle_replica_event :track with conflicting metadata and local clock dominates should keep the metadata",
       %{name: name, pid: pid, meta: meta, lclock: lclock, rclock: rclock} do
    lclock = Clock.event(lclock)
    send_replica_event(lclock, {:track, name, pid, meta})

    remote_meta = %{other: "meta"}
    send_replica_event(rclock, {:track, name, pid, remote_meta})

    assert entry(name: _, pid: ^pid, ref: _, meta: ^meta, clock: _) = Registry.get_by_name(name)
  end

  test "handle_replica_event :track with conflicting pid and remote clock dominates should kill the locally registered pid",
       %{name: name, pid: pid, meta: meta, lclock: lclock, rclock: rclock} do
    send_replica_event(lclock, {:track, name, pid, meta})

    rclock = Clock.event(rclock)
    {:ok, other_pid} = MyApp.WorkerSup.register()
    send_replica_event(rclock, {:track, name, other_pid, meta})

    assert entry(name: _, pid: ^other_pid, ref: _, meta: ^meta, clock: _) =
             Registry.get_by_name(name)

    refute Process.alive?(pid)
  end

  test "handle_replica_event :track with conflicting pid and local clock dominates should ignore the event",
       %{name: name, pid: pid, meta: meta, lclock: lclock, rclock: rclock} do

    lclock = Clock.event(lclock)
    send_replica_event(lclock, {:track, name, pid, meta})

    {:ok, other_pid} = MyApp.WorkerSup.register()
    send_replica_event(rclock, {:track, name, other_pid, meta})

    assert entry(name: _, pid: ^pid, ref: _, meta: ^meta, clock: _) = Registry.get_by_name(name)
    assert Process.alive?(pid)
  end

  test "handle_replica_event :untrack when remote clock dominates should remove registration", %{
    name: name,
    pid: pid,
    meta: meta,
    lclock: lclock,
    rclock: rclock
  } do
    send_replica_event(lclock, {:track, name, pid, meta})

    rclock = Clock.event(rclock)
    send_replica_event(rclock, {:untrack, pid})

    assert :undefined = Registry.get_by_name(name)
  end

  test "handle_replica_event :untrack when local clock dominates should ignore event", %{
    name: name,
    pid: pid,
    meta: meta,
    lclock: lclock,
    rclock: rclock
  } do
    lclock = Clock.event(lclock)
    send_replica_event(lclock, {:track, name, pid, meta})

    send_replica_event(rclock, {:untrack, pid})

    assert entry(name: _, pid: ^pid, ref: _, meta: ^meta, clock: _) = Registry.get_by_name(name)
  end

  test "handle_replica_event :untrack for unknown pid should ignore the event", %{
    name: name,
    pid: pid,
    meta: meta,
    lclock: lclock,
    rclock: rclock
  } do
    send_replica_event(lclock, {:track, name, pid, meta})

    {:ok, other_pid} = MyApp.WorkerSup.register()
    send_replica_event(rclock, {:untrack, other_pid})

    assert entry(name: _, pid: ^pid, ref: _, meta: ^meta, clock: _) = Registry.get_by_name(name)
  end

  test "handle_replica_event :update_meta for unknown pid should ignore the event", %{
    name: name,
    pid: pid,
    meta: meta,
    lclock: lclock,
    rclock: rclock
  } do
    send_replica_event(lclock, {:track, name, pid, meta})

    {:ok, other_pid} = MyApp.WorkerSup.register()
    other_meta = %{other: "meta"}
    send_replica_event(rclock, {:update_meta, other_meta, other_pid})

    assert entry(name: _, pid: ^pid, ref: _, meta: ^meta, clock: _) = Registry.get_by_name(name)
    assert :undefined = Registry.get_by_pid(other_pid)
  end

  test "handle_replica_event :update_meta when remote dominates should update the registry", %{
    name: name,
    pid: pid,
    meta: meta,
    lclock: lclock,
    rclock: rclock
  } do
    send_replica_event(lclock, {:track, name, pid, meta})

    rclock = Clock.event(rclock)
    new_meta = %{other: "meta"}
    send_replica_event(rclock, {:update_meta, new_meta, pid})

    assert entry(name: _, pid: ^pid, ref: _, meta: ^new_meta, clock: _) =
             Registry.get_by_name(name)
  end

  test "handle_replica_event :update_meta when local dominates should ignore the event", %{
    name: name,
    pid: pid,
    meta: meta,
    lclock: lclock,
    rclock: rclock
  } do
    lclock = Clock.event(lclock)
    send_replica_event(lclock, {:track, name, pid, meta})

    new_meta = %{other: "meta"}
    send_replica_event(rclock, {:update_meta, new_meta, pid})

    assert entry(name: _, pid: ^pid, ref: _, meta: ^meta, clock: _) = Registry.get_by_name(name)
  end
  
  test "handle_replica_event :update_meta when conflict should merge the meta data", %{
    name: name,
    pid: pid,
    meta: meta,
    lclock: lclock,
    rclock: rclock
  } do
    lclock = Clock.event(lclock)
    send_replica_event(lclock, {:track, name, pid, meta})

    rclock = Clock.event(rclock)
    new_meta = %{other: "meta"}
    send_replica_event(rclock, {:update_meta, new_meta, pid})

    assert entry(name: _, pid: ^pid, ref: _, meta: updated_meta, clock: _) =
             Registry.get_by_name(name)

    assert updated_meta.mfa == {MyApp.WorkerSup, :register, []}
    assert updated_meta.other == "meta"
  end

  defp send_replica_event(clock, message) do
    send(Swarm.Tracker, {:event, self(), Clock.peek(clock), message})
    # get_state to wait for the replication event to be processed
    :sys.get_state(Swarm.Tracker)
  end
end
