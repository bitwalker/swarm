## Unreleased

## Changed

- Default `node_blacklist` was expanded to ignore hot upgrade scripting
  nodes as setup by exrm/relx/distillery.
- New distribution strategy module `Swarm.Distribution.StaticQuorumRing` used to
  provide consistency during a network partition ([#38](https://github.com/bitwalker/swarm/pull/38)).

## Fixed

- When registering a name via `register_name/4` which is already registered,
  ensure the process we created via `apply/3` is killed.
- Add local registration when restarted named process is already started but unknown locally ([#46](https://github.com/bitwalker/swarm/pull/46)).

## 2.0

## Removed

- Clustering functionality, this is now provided by the `libcluster` package
- `:autocluster` config setting

## Changed

- `debug: true` now enables `:sys` tracing of the tracker, use the Logger level
  to disable `:debug` level logs when `debug: false`

## Added

- This file
