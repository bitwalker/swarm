## Next Release

### Changed

N/A

### Added

- New `Swarm.Tracker.handoff/2` function which moves all processes of a worker to the remaining ones, so the worker node can be shut down gracefully [#83](https://github.com/bitwalker/swarm/pull/83).

### Removed

N/A

### Fixed

Don't attempt to hand-off or restart processes started with `Swarm.register_name/2` ([#63](https://github.com/bitwalker/swarm/pull/63)). Fixes #62.

## 3.1

### Changed

- Default `node_blacklist` was expanded to ignore hot upgrade scripting nodes as setup by exrm/relx/distillery.

### Added

- New distribution strategy module `Swarm.Distribution.StaticQuorumRing` used to provide consistency during a network partition ([#38](https://github.com/bitwalker/swarm/pull/38)).
- Name registration error returned if no available node ([#42](https://github.com/bitwalker/swarm/pull/42)).

### Fixed

- When registering a name via `register_name/4` which is already registered,
  ensure the process we created via `apply/3` is killed.
- Remember process joined groups when nodes topology change ([#37](https://github.com/bitwalker/swarm/pull/37)).
- Retry node up when `:swarm` fails to start ([#40](https://github.com/bitwalker/swarm/pull/40)).
- Do not break local start order of application ([#43](https://github.com/bitwalker/swarm/pull/43)).
- Add local registration when restarted named process is already started but unknown locally ([#46](https://github.com/bitwalker/swarm/pull/46)).
- Retry starting remote process when module not yet available on target node ([#56](https://github.com/bitwalker/swarm/pull/56)).

## 2.0

### Removed

- Clustering functionality, this is now provided by the `libcluster` package
- `:autocluster` config setting

### Changed

- `debug: true` now enables `:sys` tracing of the tracker, use the Logger level to disable `:debug` level logs when `debug: false`

### Added

- This file
