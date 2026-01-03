# Machine Latch

[![Clojars Project](https://img.shields.io/clojars/v/co.multiply/machine-latch.svg)](https://clojars.org/co.multiply/machine-latch)
[![cljdoc](https://cljdoc.org/badge/co.multiply/machine-latch)](https://cljdoc.org/d/co.multiply/machine-latch)

A Clojure library providing a state machine latch with declarative transitions and await semantics. Designed for
coordinating concurrent operations on virtual threads.

## Requirements

- JDK 21+ (virtual threads require JDK 21)
- JDK 25+ recommended (for native `ScopedValue` support)
- Clojure 1.12+

This library depends on [co.multiply/scoped](https://github.com/multiplyco/scoped) which uses Java's `ScopedValue` API
on JDK 25+, with a fallback for older versions.

## Installation

```clojure
;; deps.edn
co.multiply/machine-latch {:mvn/version "0.1.10"}
```

## Why machine-latch?

Machine Latch came together while hunting for (and failing to find) a synchronization primitive that would succinctly
express a simple state machine.

The goal was to be able to declare a number of phases, specify valid transitions between them (and so exclude invalid
ones), while having the assurance that exactly one caller out of multiple can "win" right to perform a transition. 

- **Targeted wake-ups**: Threads waiting for e.g. phase 5 stay parked when you transition from phase 1 to phase 2.
  Only threads whose target state has been reached are woken.
- **Strict state machine contract**: Define valid states and transitions upfront. Only declared transitions are
  allowed, and exactly one caller wins each transition via CAS.
- **Small footprint**: Compiles down to a couple of `case` forms and some integer comparisons.

```clojure
(require '[co.multiply.machine-latch :as ml])

(def make-job-latch
  (ml/machine-latch-factory
    {:states      [:queued :running :complete]
     :transitions {:start  {:queued :running}
                   :finish {:running :complete}
                   :cancel {:queued  :complete
                            :running :complete}}}))
```

## API

### `machine-latch-factory`

Creates a factory function from a machine spec. The factory produces latch instances that share compiled transition
logic.

```clojure
(require '[co.multiply.machine-latch :as ml])

(def make-latch
  (ml/machine-latch-factory
    {:states      [:pending :running :done]
     :transitions {:start  {:pending :running}
                   :finish {:running :done}}}))

(def latch (make-latch))
```

Actions can have multiple source states:

```clojure
(def make-latch
  (ml/machine-latch-factory
    {:states      [:pending :running :done :cancelled]
     :transitions {:start  {:pending :running}
                   :finish {:running :done}
                   :cancel {:pending :cancelled
                            :running :cancelled}}}))
```

### `transition!`

Atomically attempt a state transition. Returns `true` if this thread won the transition, `false` otherwise.

```clojure
(def latch (make-latch))

(ml/transition! latch :start)  ; => true (won the transition)
(ml/transition! latch :start)  ; => false (already past :pending)

(ml/get-state latch)           ; => :running
```

When `transition!` returns `true`, the calling thread "owns" that phase and should proceed with the associated work.
When it returns `false`, another thread won (or the action isn't valid from the current state).

### `get-state`

Returns the current state keyword. Non-blocking.

```clojure
(ml/get-state latch)  ; => :running
```

### `at-or-past?`

Non-blocking check: has the latch reached or passed a given state?

```clojure
(ml/at-or-past? latch :pending)  ; => true (we're past it)
(ml/at-or-past? latch :running)  ; => true (we're at it)
(ml/at-or-past? latch :done)     ; => false (not yet)
```

### `await`

Block until the latch reaches or passes a target state. Returns `true` when reached.

```clojure
;; On a virtual thread:
(ml/await latch :done)  ; blocks until :done, returns true
```

Must be called from a virtual thread by default (see [Platform Thread Protection](#platform-thread-protection)).

### `await-millis` / `await-dur`

Block with a timeout. Returns `true` if the state was reached, `false` if timed out.

```clojure
(require '[co.multiply.machine-latch :as ml])
(import '[java.time Duration])

(ml/await-millis latch :done 5000)              ; 5 second timeout
(ml/await-dur latch :done (Duration/ofSeconds 5))  ; same, with Duration
```

## Constraints

Validated at compile time:

- States must be declared in order from initial to terminal
- Transitions must only go forward in this order
- The `:states` vector defines both valid states and their ordering
- The machine must have exactly one terminal state

## Execution Ownership

`transition!` grants execution ownership via CAS. When it returns `true`:

- The caller "owns" the current phase's work
- No other caller can win the same transition
- The caller may proceed with phase-specific logic

When it returns `false`, another thread won (or the transition isn't valid from the current state). The losing thread
should not proceed with that phase's work.

## Platform Thread Protection

By default, `await` throws `IllegalStateException` if called from a platform thread. Parking platform threads is a bad
idea, and is by default discouraged by this library. By throwing, you have an increased chance of discovering cases that
might be heading toward threadpool starvation.

Disable via:

- JVM property: `-Dco.multiply.machine-latch.assert-virtual=false`
- At runtime: `(ml/throw-on-platform-park! false)`
- Per-call: `(scoping [ml/*assert-virtual* false] (ml/await latch :state))` \*

\* See [scoping](https://github.com/multiplyco/scoped#scoping-clj--cljs)

## Example: Job Coordination

A job that can be started, finished, or cancelled:

```clojure
(require '[co.multiply.machine-latch :as ml])

(def make-job-latch
  (ml/machine-latch-factory
    {:states      [:queued :running :complete]
     :transitions {:start  {:queued :running}
                   :finish {:running :complete}
                   :cancel {:queued  :complete
                            :running :complete}}}))

(let [latch (make-job-latch)]
  ;; Spawn worker
  (Thread/startVirtualThread
    (fn []
      (when (ml/transition! latch :start)
        (Thread/sleep 5000)  ; simulate work
        (ml/transition! latch :finish))))

  ;; Wait for completion (on a virtual thread)
  (Thread/startVirtualThread
    (fn []
      (ml/await latch :complete)
      (println "Job complete!")))

  ;; Or cancel it
  (ml/transition! latch :cancel))
```

## License

Eclipse Public License 2.0. Copyright (c) 2025 Multiply. See [LICENSE](LICENSE).

Authored by [@eneroth](https://github.com/eneroth)
