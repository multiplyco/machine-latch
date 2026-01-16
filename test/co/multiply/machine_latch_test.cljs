(ns co.multiply.machine-latch-test
  "CLJS-specific tests for machine-latch (Promise-based await)."
  (:require
    [cljs.test :refer [deftest is testing async]]
    [co.multiply.machine-latch :as ml]
    [co.multiply.machine-latch-test.common-test]
    [co.multiply.machine-latch-test.fixtures :as fixtures]))


;; =============================================================================
;; Await - already reached (returns resolved Promise)
;; =============================================================================
(deftest test-await-already-reached
  (async done
    (let [latch (fixtures/make-simple-latch)]
      (ml/transition! latch :advance)
      (is (= :b (ml/get-state latch)))
      ;; Already at :b, should resolve immediately
      (-> (ml/await latch :b)
          (.then (fn [result]
                   (is (true? result))
                   (done)))))))


(deftest test-await-past-state
  (async done
    (let [latch (fixtures/make-simple-latch)]
      (ml/transition! latch :advance)
      (ml/transition! latch :advance)
      (is (= :c (ml/get-state latch)))
      ;; At :c, waiting for :b should succeed (already past it)
      (-> (ml/await latch :b)
          (.then (fn [result]
                   (is (true? result))
                   (done)))))))


(deftest test-await-initial-state
  (async done
    (let [latch (fixtures/make-simple-latch)]
      ;; At :a, waiting for :a should succeed
      (-> (ml/await latch :a)
          (.then (fn [result]
                   (is (true? result))
                   (done)))))))


;; =============================================================================
;; Await - pending (Promise resolves when transition occurs)
;; =============================================================================
(deftest test-await-blocks-until-target
  (async done
    (let [latch   (fixtures/make-simple-latch)
          awaited (atom false)]

      ;; Start waiting for :b
      (-> (ml/await latch :b)
          (.then (fn [result]
                   (reset! awaited true)
                   (is (true? result))
                   (is (= :b (ml/get-state latch)))
                   (done))))

      ;; Transition happens async (simulating work)
      (js/setTimeout
        (fn []
          (is (false? @awaited))  ;; Should not have resolved yet
          (ml/transition! latch :advance))
        10))))


(deftest test-await-for-terminal
  (async done
    (let [latch (fixtures/make-simple-latch)]

      ;; Wait for terminal state
      (-> (ml/await latch :c)
          (.then (fn [result]
                   (is (true? result))
                   (is (= :c (ml/get-state latch)))
                   (done))))

      ;; Transitions happen async
      (js/setTimeout #(ml/transition! latch :advance) 10)
      (js/setTimeout #(ml/transition! latch :advance) 20))))


;; =============================================================================
;; Await - timeout
;; =============================================================================
(deftest test-await-timeout-success
  (async done
    (let [latch (fixtures/make-simple-latch)]

      (-> (ml/await-millis latch :b 1000)
          (.then (fn [result]
                   (is (true? result))
                   (done))))

      (js/setTimeout #(ml/transition! latch :advance) 50))))


(deftest test-await-timeout-expires
  (async done
    (let [latch (fixtures/make-simple-latch)]

      (-> (ml/await-millis latch :c 50)
          (.then (fn [result]
                   (is (false? result))
                   (done)))))))


;; =============================================================================
;; Multiple waiters
;; =============================================================================
(deftest test-multiple-waiters-different-states
  (async done
    (let [latch   (fixtures/make-task-latch)
          results (atom [])]

      ;; Three waiters for different states
      (-> (ml/await latch :running)
          (.then (fn [_]
                   (swap! results conj [:a (ml/get-state latch)]))))

      (-> (ml/await latch :resolving)
          (.then (fn [_]
                   (swap! results conj [:b (ml/get-state latch)]))))

      (-> (ml/await latch :quiescent)
          (.then (fn [_]
                   (swap! results conj [:c (ml/get-state latch)])
                   ;; All three should have completed
                   (is (= 3 (count @results)))
                   (done))))

      ;; Transitions
      (js/setTimeout #(ml/transition! latch :run) 10)
      (js/setTimeout #(ml/transition! latch :resolve) 20)
      (js/setTimeout #(ml/transition! latch :write) 30)
      (js/setTimeout #(ml/transition! latch :settle) 40))))


(deftest test-multiple-waiters-same-state
  (async done
    (let [latch (fixtures/make-simple-latch)
          woken (atom 0)]

      ;; Three waiters for :b
      (dotimes [_ 3]
        (-> (ml/await latch :b)
            (.then (fn [_]
                     (swap! woken inc)
                     (when (= 3 @woken)
                       (done))))))

      (js/setTimeout #(ml/transition! latch :advance) 10))))


;; =============================================================================
;; Transition wakes waiters for earlier states too
;; =============================================================================
(deftest test-transition-wakes-earlier-waiters
  (async done
    (let [latch (fixtures/make-task-latch)]

      ;; Wait for :running, but we'll cancel directly to :settling
      (-> (ml/await latch :running)
          (.then (fn [result]
                   (is (true? result))
                   ;; Should have woken because :settling > :running
                   (is (= :settling (ml/get-state latch)))
                   (done))))

      ;; Cancel jumps past :running to :settling
      (js/setTimeout #(ml/transition! latch :cancel) 10))))
