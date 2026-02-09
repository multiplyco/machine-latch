(ns co.multiply.machine-latch
  (:refer-clojure :exclude [await])
  #?(:cljs (:require-macros [co.multiply.machine-latch]))
  (:require
    [co.multiply.machine-latch.common :as common]
    [co.multiply.machine-latch.impl :as impl])
  #?(:clj (:import
            [co.multiply.machine_latch.impl IMachineLatch])))



;; # CLJ-only: Platform thread assertion config
;; ################################################################################
#?(:clj
   (def ^:dynamic *assert-virtual*
     "When true (default), `await` throws if called from a platform thread.

      Disable via:
      - JVM property: `-Dco.multiply.machine-latch.assert-virtual=false`
      - At runtime: `(alter-var-root #'*assert-virtual* (constantly false))`
      - Per-call: `(scoping [ml/*assert-virtual* false] ...)`"
     (not= "false" (System/getProperty "co.multiply.machine-latch.assert-virtual"))))


#?(:clj
   (defn throw-on-platform-park!
     "Configure whether awaiting from a platform thread throws an exception.
      When true (default), parking a platform thread raises `IllegalStateException`.
      Set to false for testing or when platform thread parking is intentional.
      CLJ only."
     [bool]
     (alter-var-root #'*assert-virtual* (constantly (boolean bool)))))


;; # Factory macro
;; ################################################################################
#?(:clj
   (defmacro machine-latch-factory
     "Returns a factory fn that creates latches for this machine.

      Machine spec:

      ```clojure
      {:states [:a :b :c ...]           ; ordered from initial to terminal
       :transitions {:action {from to}  ; action with single from
                     :action2 {from1 to1, from2 to2}}}  ; action with multiple froms
      ```

      Pre-computes `state->idx` mapping and compiles transitions to use integers.
      Each latch instance has its own state and waiter collection."
     [machine]
     (let [machine (try
                     ;; Force-load the CLJS namespace in CLJ
                     (when-let [cljs-ns (:ns &env)]
                       (try (require (symbol (str (:name cljs-ns))))
                         (catch Exception _)))
                     ;; Attempt to eval machine.
                     (eval machine)
                     (catch Exception e
                       (if (:ns &env)
                         ;; CLJS: vars must be defined in .cljc files to be resolvable
                         (throw (IllegalArgumentException.
                                  (str "Cannot resolve machine spec in CLJS: `" machine "`. "
                                    "Define the machine in a .cljc file, or use a literal map.")))
                         (throw e))))]
       (common/validate-machine! machine)
       (let [states        (:states machine)
             indices       (vec (range (count states)))
             state->idx    (interleave states indices)
             idx->state    (interleave indices states)
             transitions   (:transitions machine)
             transition-fn (common/compile-transition-fn transitions (zipmap states indices))]
         `(fn make-latch# []
            (impl/make-latch
              (fn [i#] (case (unchecked-int i#) ~@idx->state))
              (fn [s#] (case s# ~@state->idx))
              ~transition-fn))))))


;; # Public API
;; ################################################################################
#?(:clj
   (defmacro get-state
     "Returns the current state keyword."
     [latch]
     (if (:ns &env)
       `(impl/-get-state ~latch)
       `(IMachineLatch/.getState ~latch))))


#?(:clj
   (defmacro at-or-past?
     "Returns true if current state >= `target-state` in declared order. Non-blocking."
     [latch target-state]
     (if (:ns &env)
       `(impl/-at-or-past? ~latch ~target-state)
       `(IMachineLatch/.atOrPast ~latch ~target-state))))


#?(:clj
   (defmacro transition!
     "Atomically attempt to perform action. Returns true if succeeded, false otherwise.
      Fails if the action is not valid from the current state."
     [latch action]
     (if (:ns &env)
       `(impl/-transition! ~latch ~action)
       `(IMachineLatch/.transition ~latch ~action))))


#?(:clj
   (defmacro await
     "CLJ: Block until state >= `target-state`. Returns true when reached.
      CLJS: Returns a Promise that resolves to true when state >= `target-state`."
     [latch target-state]
     (if (:ns &env)
       `(impl/-await ~latch ~target-state)
       `(IMachineLatch/.await ~latch ~target-state))))


#?(:clj
   (defmacro await-millis
     "CLJ: Block until state >= `target-state` or timeout. Returns true if reached, false if timed out.
      CLJS: Returns a Promise that resolves to true or false."
     [latch target-state milliseconds]
     (if (:ns &env)
       `(impl/-await-millis ~latch ~target-state ~milliseconds)
       `(IMachineLatch/.awaitMillis ~latch ~target-state ~milliseconds))))


;; CLJ-only: Duration-based await
#?(:clj
   (defmacro await-dur
     "Block until state >= `target-state` or timeout. Returns true if reached, false if timed out.
      Must be called from a virtual thread (unless `*assert-virtual*` is false).
      CLJ only."
     [latch target-state duration]
     (if (:ns &env)
       (throw (IllegalArgumentException. "await-dur is not available in CLJS. Use await-millis instead."))
       `(IMachineLatch/.awaitDur ~latch ~target-state ~duration))))