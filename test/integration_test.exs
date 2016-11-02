defmodule Swarm.IntegrationTest do
  use Swarm.NodeCase

  @primary :"primary@127.0.0.1"
  @node1 :"node1@127.0.0.1"
  @node2 :"node2@127.0.0.1"
  @worker_count 10

  test "correct redistribution of processes" do
    for n <- 1..@worker_count do
      {_, {:ok, _}} = spawn_worker(@node1, {:worker, n})
    end

    node1_registry = get_registry(@node1)
    node2_registry = get_registry(@node2)

    # each node should have a all workers in their registry
    assert length(node1_registry) == @worker_count
    assert length(node2_registry) == @worker_count

    assert length(workers_for(@node1)) < @worker_count
    assert length(workers_for(@node2)) < @worker_count

    # netsplit
    disconnect(@node1, from: @node2)

    # wait for process redistribution
    Process.sleep(@worker_count)

    ## check to see if the processes were migrated as expected
    assert length(workers_for(@node1)) == @worker_count
    assert length(workers_for(@node2)) == @worker_count

    # restore the cluster
    connect(@node1, to: @node2)

    # give time to sync
    Process.sleep(@worker_count)

    # make sure processes are back in the correct place
    assert length(workers_for(@node1)) < @worker_count
    assert length(workers_for(@node2)) < @worker_count
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

  defp workers_for(node) do
    node
    |> get_registry()
    |> Enum.filter(fn {_, pid} -> node(pid) == node end)
  end
end
