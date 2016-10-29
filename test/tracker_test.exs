defmodule Swarm.TrackerTests do
  use ExUnit.Case, async: false

  import Swarm.Entry
  alias Swarm.IntervalTreeClock, as: Clock

  @moduletag :capture_log

  setup_all do
    :rand.seed(:exs64)
    Application.ensure_all_started(:swarm)
    {:ok, _} = MyApp.WorkerSup.start_link()
    :ok
  end

  test "handle_replica_event with no prexisting reg" do
    {:ok, pid} = MyApp.WorkerSup.register()
    meta = %{mfa: {MyApp.WorkerSup, :register, []}}

    send(Swarm.Tracker, {:event, self(), Clock.seed(), {:track, :test1, pid, meta}})

    Process.sleep(5_000)

    assert ^pid = Swarm.Registry.whereis(:test1)

    all = Swarm.Registry.all()
    assert Enum.member?(all, {:test1, pid})

    assert entry(name: _, pid: ^pid, ref: ref, meta: _, clock: _) = Swarm.Registry.get_by_name(:test1)

    assert [entry(name: :test1, pid: _, ref: _, meta: _, clock: _)] = Swarm.Registry.get_by_pid(pid)

    assert entry(name: _, pid: _, ref: ^ref, meta: _, clock: _) = Swarm.Registry.get_by_pid_and_name(pid, :test1)

    assert entry(name: _, pid: ^pid, ref: _, meta: _, clock: _) = Swarm.Registry.get_by_ref(ref)

    assert [entry(pid: ^pid)] = Swarm.Registry.get_by_meta(:mfa, {MyApp.WorkerSup, :register, []})

    assert [entry(pid: ^pid)] = :ets.lookup(:swarm_registry, :test1)
  end

end
