(ns co.multiply.machine-latch.impl
  "CLJ implementation of MachineLatch."
  (:refer-clojure :exclude [await])
  (:require
    [co.multiply.scoped.impl :refer [get-scoped-var]])
  (:import
    [java.time Duration]
    [java.util.concurrent ConcurrentSkipListSet]
    [java.util.concurrent.atomic AtomicInteger]
    [java.util.concurrent.locks LockSupport]))


;; # Assert parkable
;; ################################################################################
(def ^:private assert-virtual-var
  "Resolved at runtime to avoid circular dependency."
  (delay (resolve 'co.multiply.machine-latch/*assert-virtual*)))

(defn assert-virtual!
  "Throws if current thread is a platform thread.
   Parking platform threads starves the compute pool."
  [^Thread thread]
  (when (and (get-scoped-var @assert-virtual-var)
             (not (Thread/.isVirtual thread)))
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


;; # Constructor
;; ################################################################################
(defn make-latch
  "Create a new MachineLatch instance."
  [idx->state state->idx transition-fn]
  (MachineLatch. idx->state state->idx transition-fn
    (AtomicInteger. 0)
    (ConcurrentSkipListSet.)))


