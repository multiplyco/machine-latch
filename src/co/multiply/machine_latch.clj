(ns co.multiply.machine-latch
  (:refer-clojure :exclude [await])
  (:require
    [clojure.set :as set]
    [co.multiply.scoped :refer [ask]])
  (:import
    [java.time Duration]
    [java.util.concurrent ConcurrentSkipListSet]
    [java.util.concurrent.atomic AtomicInteger]
    [java.util.concurrent.locks LockSupport]))


;; # Assert parkable
;; ################################################################################
(def ^:dynamic *assert-virtual*
  "When true (default), `await` throws if called from a platform thread.

   Disable via:
   - JVM property: `-Dco.multiply.machine-latch.assert-virtual=false`
   - At runtime: `(alter-var-root #'*assert-virtual* (constantly false))`"
  (not= "false" (System/getProperty "co.multiply.machine-latch.assert-virtual")))


(defn assert-virtual!
  "Throws if current thread is a platform thread.
   Parking platform threads starves the compute pool."
  [^Thread thread]
  (when (and (ask *assert-virtual*) (not (Thread/.isVirtual thread)))
    (throw (IllegalStateException. "Cannot park platform thread. Await from virtual thread instead."))))


;; # Waiter type
;; ################################################################################
(deftype Waiter [^Thread thread ^int target-idx]
  Comparable
  (compareTo [_ other]
    (let [c (Integer/compare target-idx (.-target-idx ^Waiter other))]
      (if (zero? c)
        (Long/compare (Thread/.threadId thread) (Thread/.threadId (.-thread ^Waiter other)))
        c))))


;; # Interface
;; ################################################################################
(definterface IMachineLatch
  (getState []
    "Returns the current state keyword.")
  (^boolean atOrPast [target-state]
                     "Returns true if current state >= target-state in declared order.")
  (^boolean atOrPastIdx [^int target-index]
                        "Returns true if current state >= target-state in declared order.")
  (^boolean transition [action]
                       "Attempt to perform action. Returns true if succeeded, false otherwise.")
  (^boolean await [target-state]
                  "Block until state >= target-state. Returns true when reached.")
  (^boolean awaitMillis [target-state ^long timeout-ms]
                        "Block until state >= target-state or timeout. Returns true if reached, false if timed out.")
  (^boolean awaitDur [target-state ^java.time.Duration timeout-dur]
                     "Block until state >= target-state or timeout. Returns true if reached, false if timed out."))


;; # Latch type
;; ################################################################################
(deftype MachineLatch [idx->state state->idx transition-fn ^AtomicInteger current-idx ^ConcurrentSkipListSet waiters]
  IMachineLatch
  (getState [_]
    (idx->state (AtomicInteger/.get current-idx)))

  (^boolean atOrPastIdx [_ ^int target-idx]
    (>= (AtomicInteger/.get current-idx) ^int target-idx))

  (^boolean atOrPast [this target-state]
    (let [target-idx (state->idx target-state)]
      (assert (some? target-idx) (str "Unknown state: " target-state))
      (IMachineLatch/.atOrPastIdx this target-idx)))

  (^boolean transition [_ action]
    (let [current (AtomicInteger/.get current-idx)
          to-idx  (transition-fn action current)]
      (if (and (some? to-idx) (AtomicInteger/.compareAndSet current-idx current to-idx))
        (or (ConcurrentSkipListSet/.isEmpty waiters)
          (let [iter (.iterator waiters)]
            (loop []
              (when (.hasNext iter)
                (let [^Waiter waiter (.next iter)]
                  (when (<= (.-target-idx waiter) to-idx)
                    (when (ConcurrentSkipListSet/.remove waiters waiter)
                      (LockSupport/unpark (.-thread waiter)))
                    (recur)))))
            true))
        false)))

  (^boolean await [this target-state]
    (IMachineLatch/.awaitMillis this target-state (long 0)))

  (^boolean awaitDur [this target-state ^java.time.Duration timeout-dur]
    (IMachineLatch/.awaitMillis this target-state (Duration/.toMillis timeout-dur)))

  (^boolean awaitMillis [this target-state ^long timeout-ms]
    (let [target-idx (state->idx target-state)]
      (assert (some? target-idx) (str "Unknown state: " target-state))
      (or (IMachineLatch/.atOrPastIdx this target-idx)
        (let [thread (Thread/currentThread)]
          (assert-virtual! thread)
          (let [waiter      (Waiter. thread target-idx)
                has-timeout (not (zero? timeout-ms))
                deadline-ns (when has-timeout
                              (+ (System/nanoTime) (* (long timeout-ms) 1000000)))]
            (ConcurrentSkipListSet/.add waiters waiter)
            (try
              (loop []
                (cond
                  (IMachineLatch/.atOrPastIdx this target-idx)
                  true

                  (Thread/.isInterrupted thread)
                  (throw (InterruptedException.))

                  has-timeout
                  (let [current-nanos (System/nanoTime)]
                    (if (<= deadline-ns current-nanos)
                      false
                      (do (LockSupport/parkNanos (- deadline-ns current-nanos))
                        (recur))))

                  :else
                  (do (LockSupport/park)
                    (recur))))
              (finally
                (ConcurrentSkipListSet/.remove waiters waiter)))))))))


;; # Factory
;; ################################################################################
(defn- terminal-states
  "Returns states that appear as destinations but never as sources.
   A valid machine has exactly one terminal state."
  [transitions]
  (let [all-froms (into #{} (mapcat keys) (vals transitions))
        all-tos   (into #{} (mapcat vals) (vals transitions))]
    (set/difference all-tos all-froms)))


(defn- validate-machine!
  "Validates machine spec. Throws on invalid spec."
  [{:keys [states transitions]}]
  (let [state-set  (set states)
        state->idx (into {} (map-indexed (fn [i s] [s i])) states)
        terminals  (terminal-states transitions)]

    ;; Must have exactly one terminal state (catches dead ends too)
    (when (not= 1 (count terminals))
      (throw (IllegalArgumentException.
               (str "Machine must have exactly one terminal state, found: " terminals))))

    ;; All states in transitions must be declared
    (doseq [[action from->to] transitions
            [from to] from->to]
      (when-not (contains? state-set from)
        (throw (IllegalArgumentException.
                 (str "Action " action " references undeclared from-state: " from))))
      (when-not (contains? state-set to)
        (throw (IllegalArgumentException.
                 (str "Action " action " references undeclared to-state: " to))))

      ;; Transitions must go forward
      (when-not (< (state->idx from) (state->idx to))
        (throw (IllegalArgumentException.
                 (str "Action " action " has backward transition: " from " -> " to)))))))


(defn- compile-transition-fn
  "Compile transitions map to a nested case function.
   Input: {action-kw {from-idx to-idx, ...}, ...}
   Output: (fn [action from-idx] (case action ...))"
  [transitions state->idx]
  (let [action-sym (gensym "action")
        from-sym   (gensym "from")]
    `(fn [~action-sym ~from-sym]
       (case ~action-sym
         ~@(mapcat (fn [[action action-transitions]]
                     [action `(case ~from-sym
                                ~@(into [] (comp cat (map state->idx)) action-transitions)
                                nil)])
             transitions)
         nil))))


(defmacro machine-latch-factory
  "Returns a factory fn that creates latches for this machine.

   Machine spec:

   ```clojure
   {:states [:a :b :c ...]           ; ordered from initial to terminal
    :transitions {:action {from to}  ; action with single from
                  :action2 {from1 to1, from2 to2}}}  ; action with multiple froms
   ```

   Pre-computes `state->idx` mapping and compiles transitions to use integers.
   Each latch instance has its own atomic state and waiter set."
  [machine]
  ;; Resolve vars in
  (let [machine (eval machine)]
    (validate-machine! machine)
    (let [states        (:states machine)
          indices       (vec (range (count states)))
          state->idx    (interleave states indices)
          idx->state    (interleave indices states)
          transitions   (:transitions machine)
          transition-fn (compile-transition-fn transitions (zipmap states indices))]
      `(fn make-latch# []
         (MachineLatch.
           (fn [i#] (case i# ~@idx->state))
           (fn [s#] (case s# ~@state->idx))
           ~transition-fn
           (AtomicInteger. 0)
           (ConcurrentSkipListSet.))))))


;; # API
;; ################################################################################
(defmacro get-state
  "Returns the current state keyword."
  [latch]
  `(IMachineLatch/.getState ~latch))


(defmacro at-or-past?
  "Returns true if current state >= `target-state` in declared order. Non-blocking."
  [latch target-state]
  `(IMachineLatch/.atOrPast ~latch ~target-state))


(defmacro transition!
  "Atomically attempt to perform action. Returns true if succeeded, false otherwise.
   Fails if the action is not valid from the current state."
  [latch action]
  `(IMachineLatch/.transition ~latch ~action))


(defmacro await
  "Block until state >= `target-state` (in declared order). Returns true when reached.
   Must be called from a virtual thread (unless `*assert-virtual*` is false)."
  [latch target-state]
  `(IMachineLatch/.await ^MachineLatch ~latch ~target-state))


(defmacro await-millis
  "Block until state >= `target-state` or timeout. Returns true if reached, false if timed out.
   Must be called from a virtual thread (unless `*assert-virtual*` is false)."
  [latch target-state milliseconds]
  `(IMachineLatch/.awaitMillis ~latch ~target-state ~milliseconds))


(defmacro await-dur
  "Block until state >= `target-state` or timeout. Returns true if reached, false if timed out.
   Must be called from a virtual thread (unless `*assert-virtual*` is false)."
  [latch target-state duration]
  `(IMachineLatch/.awaitDur ~latch ~target-state ~duration))


(defn throw-on-platform-park!
  "Configure whether awaiting from a platform thread throws an exception.
   When true (default), parking a platform thread raises `IllegalStateException`.
   Set to false for testing or when platform thread parking is intentional."
  [bool]
  (alter-var-root #'*assert-virtual* (constantly (boolean bool))))
