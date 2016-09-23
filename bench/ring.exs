Application.ensure_started(:swarm)

ring = Swarm.Ring.new(:'a@nohost')
ring = Swarm.Ring.add_node(ring, :'b@nohost')

# NOTE: Uncomment the following after adding voicelayer/hash-ring to your deps
# In my tests, Swarm.Ring performs 5-10x better at mapping keys to to the ring
# I'm not entirely sure why this is, as hash_ring is implemented with a NIF, but I
# suspect my gb_tree based implementation is just much more efficient at searching the
# ring, and hash_ring uses a naive search or something, anyway, feel free to test yourself
#_ = :hash_ring.start_link()
#:hash_ring.create_ring("test", 128)
#:hash_ring.add_node("test", "a@nohost")
#:hash_ring.add_node("test", "b@nohost")

Benchee.run(%{time: 10}, %{
      "Swarm.Ring.key_to_node" => fn ->
        for i <- 1..100, do: Swarm.Ring.key_to_node(ring, {:myapp, i})
      end,
      #"hash_ring.find_node" => fn ->
      #  for i <- 1..100, do: :hash_ring.find_node("test", :erlang.term_to_binary({:myapp, i}))
      #end
})
