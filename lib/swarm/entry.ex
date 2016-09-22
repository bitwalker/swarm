defmodule Swarm.Entry do
  @moduledoc false
  alias Swarm.IntervalTreeClock, as: ITC

  require Record
  Record.defrecord :entry,
    name: nil,
    pid: nil,
    ref: nil,
    meta: nil,
    clock: nil

  @type entry :: record(:entry, name: term, pid: pid, ref: reference, meta: nil | Map.t, clock: nil | ITC.t)
end
