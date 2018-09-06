defmodule Swarm.IntervalTreeClockTests do
  use ExUnit.Case

  alias Swarm.IntervalTreeClock, as: Clock

  setup do
    lclock = Clock.seed()
    {lclock, rclock} = Clock.fork(lclock)
    [lclock: lclock, rclock: rclock]
  end

  test "Forked clocks compare equal", %{lclock: lclock, rclock: rclock} do
    assert Clock.compare(lclock, rclock) == :eq
    assert Clock.leq(lclock, rclock)
    assert Clock.leq(rclock, lclock)
  end

  test "Peeked Clock has zero identity and still compares equal", %{
    lclock: lclock,
    rclock: rclock
  } do
    rclock = Clock.peek(rclock)

    {0, _} = rclock
    assert Clock.compare(lclock, rclock) == :eq
    assert Clock.leq(lclock, rclock)
    assert Clock.leq(rclock, lclock)
  end

  test "Clock with additional event is greater", %{lclock: lclock, rclock: rclock} do
    rclock = Clock.event(rclock)

    assert Clock.compare(rclock, lclock) == :gt
    assert Clock.compare(lclock, rclock) == :lt
    assert Clock.leq(lclock, rclock)
    assert not Clock.leq(rclock, lclock)
  end

  test "Peeked Clock with additional event is greater", %{lclock: lclock, rclock: rclock} do
    rclock = Clock.peek(Clock.event(rclock))

    assert Clock.compare(rclock, lclock) == :gt
    assert Clock.compare(lclock, rclock) == :lt
    assert Clock.leq(lclock, rclock)
    assert not Clock.leq(rclock, lclock)
  end

  test "Clocks with two parallel events are concurrent", %{lclock: lclock, rclock: rclock} do
    lclock = Clock.event(lclock)
    rclock = Clock.event(rclock)

    assert not Clock.leq(lclock, rclock)
    assert not Clock.leq(rclock, lclock)
    assert Clock.compare(lclock, rclock) == :concurrent
  end

  test "Peeked Clocks with two parallel events are concurrent", %{lclock: lclock, rclock: rclock} do
    lclock = Clock.peek(Clock.event(lclock))
    rclock = Clock.peek(Clock.event(rclock))

    assert not Clock.leq(lclock, rclock)
    assert not Clock.leq(rclock, lclock)
    assert Clock.compare(lclock, rclock) == :concurrent
  end

  test "Clock with additional event can be joined", %{lclock: lclock, rclock: rclock} do
    rclock = Clock.event(rclock)
    joined_clock = Clock.join(lclock, rclock)

    assert Clock.compare(joined_clock, rclock) == :eq
    assert Clock.compare(joined_clock, lclock) == :gt
  end

  test "Peeked clock with additional event can be joined", %{lclock: lclock, rclock: rclock} do
    rclock = Clock.peek(Clock.event(rclock))
    joined_clock = Clock.join(lclock, rclock)

    assert Clock.compare(joined_clock, rclock) == :eq
    assert Clock.compare(joined_clock, lclock) == :gt
  end

  test "Concurrent clocks can be joined and the joined clock contains events from both", %{
    lclock: lclock,
    rclock: rclock
  } do
    rclock = Clock.event(rclock)
    lclock = Clock.event(lclock)
    joined_clock = Clock.join(lclock, rclock)

    assert Clock.compare(joined_clock, rclock) == :gt
    assert Clock.compare(joined_clock, lclock) == :gt
  end

  test "Concurrent clocks can be joined and new event is not concurrent anymore", %{
    lclock: lclock,
    rclock: rclock
  } do
    rclock = Clock.event(rclock)
    lclock = Clock.event(lclock)

    joined_lclock = Clock.join(lclock, rclock)
    joined_rclock = Clock.join(rclock, lclock)

    joined_lclock = Clock.event(joined_lclock)

    assert Clock.compare(joined_lclock, joined_rclock) == :gt
  end
end
