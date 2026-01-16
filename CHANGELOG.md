# Changelog

## 0.1.13 - 2026-01-30

- Add ClojureScript version with Promise-based await semantics.
  - `await` and `await-millis` return Promises that resolve to `true` when the target state is reached.
  - `await-dur` is CLJ-only (throws at compile time in CLJS).
  - Machine specs must be defined in `.cljc` files or as literal maps.
- Bump `scoped` to 0.1.15.

## 0.1.12 - 2026-01-07

- Bump `scoped` to 0.1.14

## 0.1.10 - 2026-01-03

- Bump `scoped` to 0.1.13

## 0.1.8 - 2026-01-02

Initial release.

- `machine-latch-factory` - create a factory for latches from a machine spec
- `transition!` - atomically attempt a state transition
- `get-state` - get current state keyword
- `at-or-past?` - non-blocking state check
- `await` - block until target state reached
- `await-millis` - await with millisecond timeout
- `await-dur` - await with Duration timeout
- `throw-on-platform-park!` - configure platform thread protection
- `*assert-virtual*` - platform thread protection dynamic var