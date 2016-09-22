Application.ensure_started(:swarm)

ring = Swarm.Ring.new(:'a@nohost')
ring = Swarm.Ring.add_node(ring, :'b@nohost')

Benchee.run(%{time: 10}, %{
      "ring.key_to_node" => fn ->
        for i <- 1..100, do: Swarm.Ring.key_to_node(ring, {:myapp, i})
      end
})
