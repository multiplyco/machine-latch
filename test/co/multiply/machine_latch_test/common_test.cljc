(ns co.multiply.machine-latch-test.common-test
  "Shared synchronous tests for machine-latch (run on both CLJ and CLJS)."
  (:require
    #?(:clj  [clojure.test :refer [deftest is testing]]
       :cljs [cljs.test :refer [deftest is testing]])
    [co.multiply.machine-latch :as ml]
    [co.multiply.machine-latch-test.fixtures :as fixtures]))


;; =============================================================================
;; Factory validation
;; =============================================================================
(deftest test-factory-validation
  (testing "valid machine spec creates factory"
    (is (fn? fixtures/make-simple-latch))
    (is (fn? fixtures/make-task-latch))))


;; =============================================================================
;; Initial state
;; =============================================================================
(deftest test-initial-state
  (testing "latch starts at first declared state"
    (let [latch (fixtures/make-simple-latch)]
      (is (= :a (ml/get-state latch)))))

  (testing "each latch instance has independent state"
    (let [latch1 (fixtures/make-simple-latch)
          latch2 (fixtures/make-simple-latch)]
      (ml/transition! latch1 :advance)
      (is (= :b (ml/get-state latch1)))
      (is (= :a (ml/get-state latch2))))))


;; =============================================================================
;; Basic transitions
;; =============================================================================
(deftest test-transition-basic
  (testing "valid transition succeeds and changes state"
    (let [latch (fixtures/make-simple-latch)]
      (is (= :a (ml/get-state latch)))
      (is (true? (ml/transition! latch :advance)))
      (is (= :b (ml/get-state latch)))
      (is (true? (ml/transition! latch :advance)))
      (is (= :c (ml/get-state latch)))))

  (testing "transition from wrong state fails"
    (let [latch (fixtures/make-task-latch)]
      ;; Try to resolve from pending (should fail, need to run first)
      (is (false? (ml/transition! latch :resolve)))
      (is (= :pending (ml/get-state latch)))))

  (testing "transition from terminal state fails"
    (let [latch (fixtures/make-simple-latch)]
      (ml/transition! latch :advance)
      (ml/transition! latch :advance)
      (is (= :c (ml/get-state latch)))
      ;; No valid transitions from terminal state
      (is (false? (ml/transition! latch :advance)))))

  (testing "unknown action returns false"
    (let [latch (fixtures/make-simple-latch)]
      (is (false? (ml/transition! latch :unknown-action))))))


;; =============================================================================
;; Multi-source actions
;; =============================================================================
(deftest test-multi-source-actions
  (testing "cancel from pending goes to settling"
    (let [latch (fixtures/make-task-latch)]
      (is (true? (ml/transition! latch :cancel)))
      (is (= :settling (ml/get-state latch)))))

  (testing "cancel from running goes to settling"
    (let [latch (fixtures/make-task-latch)]
      (ml/transition! latch :run)
      (is (= :running (ml/get-state latch)))
      (is (true? (ml/transition! latch :cancel)))
      (is (= :settling (ml/get-state latch)))))

  (testing "cancel from resolving goes to settling"
    (let [latch (fixtures/make-task-latch)]
      (ml/transition! latch :run)
      (ml/transition! latch :resolve)
      (is (= :resolving (ml/get-state latch)))
      (is (true? (ml/transition! latch :cancel)))
      (is (= :settling (ml/get-state latch)))))

  (testing "cancel from settling fails (not a valid source)"
    (let [latch (fixtures/make-task-latch)]
      (ml/transition! latch :cancel)
      (is (= :settling (ml/get-state latch)))
      (is (false? (ml/transition! latch :cancel))))))


;; =============================================================================
;; at-or-past? (non-blocking state check)
;; =============================================================================
(deftest test-at-or-past
  (testing "at-or-past? returns true for current state"
    (let [latch (fixtures/make-simple-latch)]
      (is (true? (ml/at-or-past? latch :a)))
      (ml/transition! latch :advance)
      (is (true? (ml/at-or-past? latch :b)))))

  (testing "at-or-past? returns true for past states"
    (let [latch (fixtures/make-simple-latch)]
      (ml/transition! latch :advance)
      (ml/transition! latch :advance)
      (is (= :c (ml/get-state latch)))
      ;; At :c, both :a and :b are past
      (is (true? (ml/at-or-past? latch :a)))
      (is (true? (ml/at-or-past? latch :b)))
      (is (true? (ml/at-or-past? latch :c)))))

  (testing "at-or-past? returns false for future states"
    (let [latch (fixtures/make-simple-latch)]
      (is (= :a (ml/get-state latch)))
      (is (false? (ml/at-or-past? latch :b)))
      (is (false? (ml/at-or-past? latch :c))))))
