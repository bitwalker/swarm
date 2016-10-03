## Unreleased

## Changed

- Default `node_blacklist` was expanded to ignore hot upgrade scripting
  nodes as setup by exrm/relx/distillery.

## Fixed

- When registering a name via `register_name/4` which is already registered,
  ensure the process we created via `apply/3` is killed.
  
## 2.0

## Removed

- Clustering functionality, this is now provided by the `libcluster` package
- `:autocluster` config setting

## Changed

- `debug: true` now enables `:sys` tracing of the tracker, use the Logger level
  to disable `:debug` level logs when `debug: false`

## Added

- This file
