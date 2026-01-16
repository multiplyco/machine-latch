(ns co.multiply.machine-latch-test.fixtures
  "Shared test fixtures for machine-latch tests.
   Defined in CLJC so machine specs are resolvable at compile time in both CLJ and CLJS."
  (:require [co.multiply.machine-latch :as ml]))


;; =============================================================================
;; Machine specs
;; =============================================================================
(def simple-machine
  "Simple linear state machine: a -> b -> c"
  {:states      [:a :b :c]
   :transitions {:advance {:a :b, :b :c}}})


(def task-machine
  "Task-like machine with cancellation from multiple states"
  {:states      [:pending :running :resolving :settling :quiescent]
   :transitions {:run     {:pending :running}
                 :resolve {:running :resolving}
                 :write   {:resolving :settling}
                 :settle  {:settling :quiescent}
                 :cancel  {:pending   :settling
                           :running   :settling
                           :resolving :settling}}})


;; =============================================================================
;; Pre-compiled factories (for use in tests)
;; =============================================================================
(def make-simple-latch
  "Factory for simple-machine latches."
  (ml/machine-latch-factory simple-machine))


(def make-task-latch
  "Factory for task-machine latches."
  (ml/machine-latch-factory task-machine))
