# Distable

This project is an attempt at a smart distributed registry, which automatically
shifts processes around based on changes to cluster topology. It is designed
for Elixir and Erlang apps built on OTP conventions.

## Installation

```elixir
defp deps do
  [{:distable, "~> 0.1.0"}]
end
```

## License

MIT

## TODO

- distable
- gossip protocol via configurable port/subnet
- look into phoenix presence for cluster membership
- look into phoenix presence crdt in general to see if it's a fit
  for changes to cluster, registry
- look into riak ring implementation for ideas around hashing/distribution
  of work
- when reassigning processes, tell remote node what names/mfa's to run
  and let the node be responsible for calling the mfa for starting the
  proc
- consider implementing the `via` tuple for registration if possible
