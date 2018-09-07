defmodule Swarm.IntegrationTest do
  use Swarm.NodeCase

  @node1 :"node1@127.0.0.1"
  @node2 :"node2@127.0.0.1"
  @nodes [@node1, @node2]
  @worker_count 10

  setup do
    on_exit(fn ->
      for {_name, pid} <- get_registry(@node1), do: shutdown(pid)
    end)

    :ok
  end

  test "correct redistribution of processes" do
    group_name = :group1

    for n <- 1..@worker_count do
      {_, {:ok, _}} = spawn_worker(@node1, {:worker, n}, group_name)
    end

    # wait for process registration
    Process.sleep(1000)

    node1_registry = get_registry(@node1)
    node2_registry = get_registry(@node2)

    # each node should have all workers in their registry
    assert length(node1_registry) == @worker_count
    assert length(node2_registry) == @worker_count

    assert length(get_group_members(@node1, group_name)) == @worker_count
    assert length(get_group_members(@node2, group_name)) == @worker_count

    assert length(workers_for(@node1)) < @worker_count
    assert length(workers_for(@node2)) < @worker_count

    assert length(members_for(@node1, group_name)) < @worker_count
    assert length(members_for(@node2, group_name)) < @worker_count

    # netsplit
    disconnect(@node1, from: @node2)

    # wait for process redistribution
    Process.sleep(1000)

    ## check to see if the processes were migrated as expected
    assert length(workers_for(@node1)) == @worker_count
    assert length(workers_for(@node2)) == @worker_count

    assert length(get_group_members(@node1, group_name)) == @worker_count
    assert length(get_group_members(@node2, group_name)) == @worker_count

    # restore the cluster
    connect(@node1, to: @node2)

    # give time to sync
    Process.sleep(1000)

    # make sure processes are back in the correct place
    assert length(workers_for(@node1)) < @worker_count
    assert length(workers_for(@node2)) < @worker_count

    assert length(get_group_members(@node1, group_name)) == @worker_count
    assert length(get_group_members(@node2, group_name)) == @worker_count

    assert length(members_for(@node1, group_name)) < @worker_count
    assert length(members_for(@node2, group_name)) < @worker_count
  end

  test "redistribute already started process" do
    {_, {:ok, pid1}} = spawn_restart_worker(@node1, {:worker, 1})
    {_, {:ok, pid2}} = spawn_restart_worker(@node1, {:worker, 2})

    Enum.each(@nodes, fn node ->
      assert ordered_registry(node) == [{{:worker, 1}, pid1}, {{:worker, 2}, pid2}]
    end)

    # netsplit
    simulate_disconnect(@node1, @node2)

    # wait for process redistribution
    Process.sleep(1_000)

    # both worker processes should be running on each node
    assert whereis_name(@node1, {:worker, 1}) != whereis_name(@node2, {:worker, 1})
    assert whereis_name(@node1, {:worker, 2}) != whereis_name(@node2, {:worker, 2})

    Enum.each(@nodes, fn node ->
      # both nodes should be aware of two workers
      assert node |> get_registry() |> length() == 2
    end)

    # restore the cluster
    simulate_reconnect(@node1, @node2)

    # give time to sync
    Process.sleep(1_000)

    pid1 = whereis_name(@node1, {:worker, 1})
    pid2 = whereis_name(@node1, {:worker, 2})

    Enum.each(@nodes, fn node ->
      assert ordered_registry(node) == [{{:worker, 1}, pid1}, {{:worker, 2}, pid2}]
    end)

    shutdown(pid1)
    shutdown(pid2)

    # give time to sync
    Process.sleep(1_000)
  end

  test "don't attempt to redistribute processes started with `Swarm.register_name/2`" do
    name = {:agent, 1}
    {_, :yes} = spawn_agent(@node1, name, [])

    assert [{^name, pid}] = get_registry(@node1)

    # another node joins cluster
    Swarm.Cluster.spawn_node(:"node3@127.0.0.1")

    # ensure process still running on node1
    assert whereis_name(@node1, name) == pid

    shutdown(pid)
    Swarm.Cluster.stop_node(:"node3@127.0.0.1")
  end

  test "remove processes started with `Swarm.register_name/2` when hosting node goes down" do
    name = {:agent, 1}
    {_, :yes} = spawn_agent(@node1, name, [])

    # give time to sync
    Process.sleep(1_000)

    assert [{^name, pid}] = get_registry(@node1)
    assert [{^name, ^pid}] = get_registry(@node2)

    # stop agent process
    shutdown(pid)

    # ensure process is removed from node registries
    assert whereis_name(@node1, name) == :undefined
    assert whereis_name(@node2, name) == :undefined
  end

  defp disconnect(node, opts) do
    from = Keyword.fetch!(opts, :from)
    :rpc.call(node, Node, :disconnect, [from])
  end

  defp connect(node, opts) do
    to = Keyword.fetch!(opts, :to)
    :rpc.call(node, Node, :connect, [to])
  end

  defp get_registry(node) do
    :rpc.call(node, Swarm, :registered, [], :infinity)
  end

  defp whereis_name(node, name) do
    :rpc.call(node, Swarm, :whereis_name, [name], :infinity)
  end

  defp ordered_registry(node) do
    node
    |> get_registry()
    |> Enum.sort_by(fn {name, _pid} -> name end)
  end

  # simulate a disconnect between two nodes
  defp simulate_disconnect(lnode, rnode) do
    spawn(fn -> send({Swarm.Tracker, lnode}, {:nodedown, rnode, nil}) end)
    spawn(fn -> send({Swarm.Tracker, rnode}, {:nodedown, lnode, nil}) end)
  end

  # simulate a reconnect between two nodes
  defp simulate_reconnect(lnode, rnode) do
    spawn(fn -> send({Swarm.Tracker, lnode}, {:nodeup, rnode, nil}) end)
    spawn(fn -> send({Swarm.Tracker, rnode}, {:nodeup, lnode, nil}) end)
  end

  defp workers_for(node) do
    node
    |> get_registry()
    |> Enum.filter(fn {_, pid} -> node(pid) == node end)
  end

  defp get_group_members(node, group_name) do
    :rpc.call(node, Swarm, :members, [group_name])
  end

  defp members_for(node, group_name) do
    node
    |> get_group_members(group_name)
    |> Enum.filter(fn pid -> node(pid) == node end)
  end

  def shutdown(nil), do: :ok

  def shutdown(pid) when is_pid(pid) do
    ref = Process.monitor(pid)

    Process.unlink(pid)
    Process.exit(pid, :shutdown)

    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
