(ns co.multiply.machine-latch-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [co.multiply.machine-latch :as ml]
    [co.multiply.scoped :refer [scoping]])
  (:import
    [java.time Duration]
    [java.util.concurrent CountDownLatch TimeUnit]
    [java.util.concurrent.atomic AtomicInteger]))


(defn- run-virtual
  "Run f on a virtual thread, return the thread."
  ^Thread [f]
  (Thread/startVirtualThread f))


(defn- await-virtual
  "Run f on a virtual thread, wait for completion, return result."
  [f]
  (let [result (promise)
        error  (promise)]
    (Thread/startVirtualThread
      (fn []
        (try
          (deliver result (f))
          (catch Throwable t
            (deliver error t)))))
    (let [r (deref result 5000 ::timeout)
          e (deref error 0 nil)]
      (cond
        e (throw e)
        (= r ::timeout) (throw (ex-info "Virtual thread timed out" {}))
        :else r))))


;; =============================================================================
;; Test fixtures - common machine specs
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
;; Factory validation
;; =============================================================================
(deftest test-factory-validation
  (testing "valid machine spec creates factory"
    (let [factory (ml/machine-latch-factory simple-machine)]
      (is (fn? factory))))

  ;; Validation tests use eval to defer macro expansion to test runtime.
  (testing "rejects machine with no terminal state"
    (is (thrown? Exception
          (eval '(ml/machine-latch-factory
                   {:states      [:a :b]
                    :transitions {:loop {:a :b, :b :a}}})))))

  (testing "rejects machine with multiple terminal states"
    (is (thrown? Exception
          (eval '(ml/machine-latch-factory
                   {:states      [:a :b :c]
                    :transitions {:to-b {:a :b}
                                  :to-c {:a :c}}})))))

  (testing "rejects backward transitions"
    (is (thrown? Exception
          (eval '(ml/machine-latch-factory
                   {:states      [:a :b :c]
                    :transitions {:forward  {:a :b}
                                  :backward {:b :a}
                                  :finish   {:b :c}}})))))

  (testing "rejects undeclared from-state"
    (is (thrown? Exception
          (eval '(ml/machine-latch-factory
                   {:states      [:a :b]
                    :transitions {:advance {:x :b}}})))))

  (testing "rejects undeclared to-state"
    (is (thrown? Exception
          (eval '(ml/machine-latch-factory
                   {:states      [:a :b]
                    :transitions {:advance {:a :x}}}))))))


;; =============================================================================
;; Initial state
;; =============================================================================
(deftest test-initial-state
  (testing "latch starts at first declared state"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)]
      (is (= :a (ml/get-state latch)))))

  (testing "each latch instance has independent state"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch1     (make-latch)
          latch2     (make-latch)]
      (ml/transition! latch1 :advance)
      (is (= :b (ml/get-state latch1)))
      (is (= :a (ml/get-state latch2))))))


;; =============================================================================
;; Basic transitions
;; =============================================================================
(deftest test-transition-basic
  (testing "valid transition succeeds and changes state"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)]
      (is (= :a (ml/get-state latch)))
      (is (true? (ml/transition! latch :advance)))
      (is (= :b (ml/get-state latch)))
      (is (true? (ml/transition! latch :advance)))
      (is (= :c (ml/get-state latch)))))

  (testing "transition from wrong state fails"
    (let [make-latch (ml/machine-latch-factory task-machine)
          latch      (make-latch)]
      ;; Try to resolve from pending (should fail, need to run first)
      (is (false? (ml/transition! latch :resolve)))
      (is (= :pending (ml/get-state latch)))))

  (testing "transition from terminal state fails"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)]
      (ml/transition! latch :advance)
      (ml/transition! latch :advance)
      (is (= :c (ml/get-state latch)))
      ;; No valid transitions from terminal state
      (is (false? (ml/transition! latch :advance)))))

  (testing "unknown action returns false"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)]
      (is (false? (ml/transition! latch :unknown-action))))))


;; =============================================================================
;; Multi-source actions
;; =============================================================================
(deftest test-multi-source-actions
  (testing "cancel from pending goes to settling"
    (let [make-latch (ml/machine-latch-factory task-machine)
          latch      (make-latch)]
      (is (true? (ml/transition! latch :cancel)))
      (is (= :settling (ml/get-state latch)))))

  (testing "cancel from running goes to settling"
    (let [make-latch (ml/machine-latch-factory task-machine)
          latch      (make-latch)]
      (ml/transition! latch :run)
      (is (= :running (ml/get-state latch)))
      (is (true? (ml/transition! latch :cancel)))
      (is (= :settling (ml/get-state latch)))))

  (testing "cancel from resolving goes to settling"
    (let [make-latch (ml/machine-latch-factory task-machine)
          latch      (make-latch)]
      (ml/transition! latch :run)
      (ml/transition! latch :resolve)
      (is (= :resolving (ml/get-state latch)))
      (is (true? (ml/transition! latch :cancel)))
      (is (= :settling (ml/get-state latch)))))

  (testing "cancel from settling fails (not a valid source)"
    (let [make-latch (ml/machine-latch-factory task-machine)
          latch      (make-latch)]
      (ml/transition! latch :cancel)
      (is (= :settling (ml/get-state latch)))
      (is (false? (ml/transition! latch :cancel))))))


;; =============================================================================
;; Concurrent transitions
;; =============================================================================
(deftest test-concurrent-transitions
  (testing "concurrent transitions - exactly one wins"
    (let [;; Use task-machine with :run action which only has one source (:pending)
          make-latch   (ml/machine-latch-factory task-machine)
          latch        (make-latch)
          start-signal (CountDownLatch. 1)
          done-signal  (CountDownLatch. 10)
          successes    (AtomicInteger. 0)]

      ;; 10 threads all trying to transition :run from :pending
      (dotimes [_ 10]
        (run-virtual
          (fn []
            (CountDownLatch/.await start-signal)
            (when (ml/transition! latch :run)
              (AtomicInteger/.incrementAndGet successes))
            (CountDownLatch/.countDown done-signal))))

      (CountDownLatch/.countDown start-signal)
      (CountDownLatch/.await done-signal 5 TimeUnit/SECONDS)

      ;; Exactly one thread should have won
      (is (= 1 (AtomicInteger/.get successes)))
      (is (= :running (ml/get-state latch))))))


;; =============================================================================
;; at-or-past? (non-blocking state check)
;; =============================================================================
(deftest test-at-or-past
  (testing "at-or-past? returns true for current state"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)]
      (is (true? (ml/at-or-past? latch :a)))
      (ml/transition! latch :advance)
      (is (true? (ml/at-or-past? latch :b)))))

  (testing "at-or-past? returns true for past states"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)]
      (ml/transition! latch :advance)
      (ml/transition! latch :advance)
      (is (= :c (ml/get-state latch)))
      ;; At :c, both :a and :b are past
      (is (true? (ml/at-or-past? latch :a)))
      (is (true? (ml/at-or-past? latch :b)))
      (is (true? (ml/at-or-past? latch :c)))))

  (testing "at-or-past? returns false for future states"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)]
      (is (= :a (ml/get-state latch)))
      (is (false? (ml/at-or-past? latch :b)))
      (is (false? (ml/at-or-past? latch :c)))))

  (testing "at-or-past? throws for unknown state"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)]
      (is (thrown? IllegalArgumentException (ml/at-or-past? latch :unknown))))))


;; =============================================================================
;; Await - already reached
;; =============================================================================
(deftest test-await-already-reached
  (testing "await returns true immediately if already at target state"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)]
      (ml/transition! latch :advance)
      (is (= :b (ml/get-state latch)))
      ;; Already at :b, should return immediately
      (is (true? (await-virtual #(ml/await latch :b))))))

  (testing "await returns true immediately if past target state"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)]
      (ml/transition! latch :advance)
      (ml/transition! latch :advance)
      (is (= :c (ml/get-state latch)))
      ;; At :c, waiting for :b should succeed (already past it)
      (is (true? (await-virtual #(ml/await latch :b))))))

  (testing "await returns true immediately for initial state"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)]
      ;; At :a, waiting for :a should succeed
      (is (true? (await-virtual #(ml/await latch :a)))))))


;; =============================================================================
;; Await - blocking
;; =============================================================================
(deftest test-await-blocks-until-target
  (testing "await blocks until target state reached"
    (let [make-latch (ml/machine-latch-factory task-machine)
          latch      (make-latch)
          started    (CountDownLatch. 1)
          result     (atom nil)]

      (run-virtual
        (fn []
          (CountDownLatch/.countDown started)
          (ml/await latch :resolving)
          (reset! result (ml/get-state latch))))

      (CountDownLatch/.await started 1 TimeUnit/SECONDS)
      (Thread/sleep 50)

      ;; Still waiting
      (ml/transition! latch :run)
      (Thread/sleep 10)
      (is (nil? @result))

      ;; Now reaches resolving
      (ml/transition! latch :resolve)
      (Thread/sleep 50)
      (is (= :resolving @result))))

  (testing "await for terminal state blocks until complete"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)
          result     (promise)]

      (run-virtual
        (fn []
          (ml/await latch :c)
          (deliver result (ml/get-state latch))))

      (Thread/sleep 50)
      (is (not (realized? result)))

      (ml/transition! latch :advance)
      (Thread/sleep 10)
      (is (not (realized? result)))

      (ml/transition! latch :advance)
      (is (= :c (deref result 500 :not-delivered))))))


;; =============================================================================
;; Await - timeout
;; =============================================================================
(deftest test-await-with-timeout-success
  (testing "await-millis returns true when target reached before timeout"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)
          result     (promise)]

      (run-virtual
        (fn []
          (deliver result (ml/await-millis latch :b 1000))))

      (Thread/sleep 50)
      (ml/transition! latch :advance)

      (is (true? (deref result 500 :not-delivered)))))

  (testing "await-dur returns true when target reached before timeout"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)
          result     (promise)]

      (run-virtual
        (fn []
          (deliver result (ml/await-dur latch :c (Duration/ofSeconds 5)))))

      (Thread/sleep 50)
      (ml/transition! latch :advance)
      (ml/transition! latch :advance)

      (is (true? (deref result 500 :not-delivered))))))


(deftest test-await-with-timeout-expires
  (testing "await-millis returns false when timeout expires"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)
          result     (await-virtual
                       #(ml/await-millis latch :c 50))]
      (is (false? result))))

  (testing "await-dur returns false when timeout expires"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)
          result     (await-virtual
                       #(ml/await-dur latch :c (Duration/ofMillis 50)))]
      (is (false? result)))))


;; =============================================================================
;; Platform thread assertion
;; =============================================================================
(deftest test-await-platform-thread-assertion
  (testing "await throws on platform thread when assertion enabled"
    (scoping [ml/*assert-virtual* true]
      (let [make-latch (ml/machine-latch-factory simple-machine)
            latch      (make-latch)]
        (is (thrown-with-msg? IllegalStateException
              #"Cannot park"
              (ml/await latch :c))))))

  (testing "await succeeds on platform thread when assertion disabled"
    (scoping [ml/*assert-virtual* false]
      (let [make-latch (ml/machine-latch-factory simple-machine)
            latch      (make-latch)
            result     (promise)]
        ;; Start transitions in background
        (run-virtual
          (fn []
            (Thread/sleep 50)
            (ml/transition! latch :advance)
            (ml/transition! latch :advance)))
        ;; This runs on platform thread but should not throw
        (deliver result (ml/await-millis latch :c 500))
        (is (true? @result))))))


;; =============================================================================
;; Multiple waiters
;; =============================================================================
(deftest test-multiple-waiters-different-states
  (testing "multiple waiters wake at their respective target states"
    (let [make-latch (ml/machine-latch-factory task-machine)
          latch      (make-latch)
          started    (CountDownLatch. 3)
          results    (atom [])]

      ;; Waiter for :running
      (run-virtual
        (fn []
          (CountDownLatch/.countDown started)
          (ml/await latch :running)
          (swap! results conj [:a (ml/get-state latch)])))

      ;; Waiter for :resolving
      (run-virtual
        (fn []
          (CountDownLatch/.countDown started)
          (ml/await latch :resolving)
          (swap! results conj [:b (ml/get-state latch)])))

      ;; Waiter for :quiescent
      (run-virtual
        (fn []
          (CountDownLatch/.countDown started)
          (ml/await latch :quiescent)
          (swap! results conj [:c (ml/get-state latch)])))

      (CountDownLatch/.await started 1 TimeUnit/SECONDS)
      (Thread/sleep 50)

      (ml/transition! latch :run)
      (Thread/sleep 50)
      (is (some #(= :a (first %)) @results))

      (ml/transition! latch :resolve)
      (Thread/sleep 50)
      (is (some #(= :b (first %)) @results))

      (ml/transition! latch :write)
      (ml/transition! latch :settle)
      (Thread/sleep 50)
      (is (= 3 (count @results))))))


(deftest test-multiple-waiters-same-state
  (testing "multiple waiters for same state all wake"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)
          started    (CountDownLatch. 3)
          woken      (AtomicInteger. 0)]

      (dotimes [_ 3]
        (run-virtual
          (fn []
            (CountDownLatch/.countDown started)
            (ml/await latch :b)
            (AtomicInteger/.incrementAndGet woken))))

      (CountDownLatch/.await started 1 TimeUnit/SECONDS)
      (Thread/sleep 50)

      (ml/transition! latch :advance)
      (Thread/sleep 100)

      (is (= 3 (AtomicInteger/.get woken))))))


;; =============================================================================
;; Interrupt handling
;; =============================================================================
(deftest test-await-interrupt
  (testing "await throws InterruptedException when interrupted"
    (let [make-latch  (ml/machine-latch-factory simple-machine)
          latch       (make-latch)
          started     (CountDownLatch. 1)
          interrupted (promise)
          t           (run-virtual
                        (fn []
                          (CountDownLatch/.countDown started)
                          (try
                            (ml/await latch :c)
                            (deliver interrupted false)
                            (catch InterruptedException _
                              (deliver interrupted true)))))]

      (CountDownLatch/.await started 1 TimeUnit/SECONDS)
      (Thread/sleep 50)
      (Thread/.interrupt t)

      (is (= true (deref interrupted 1000 :not-delivered))))))


;; =============================================================================
;; Transition wakes waiters
;; =============================================================================
(deftest test-transition-wakes-waiters
  (testing "transition wakes waiters whose target is reached"
    (let [make-latch (ml/machine-latch-factory simple-machine)
          latch      (make-latch)
          started    (CountDownLatch. 1)
          result     (promise)]

      (run-virtual
        (fn []
          (CountDownLatch/.countDown started)
          (ml/await latch :b)
          (deliver result :woken)))

      (CountDownLatch/.await started 1 TimeUnit/SECONDS)
      (Thread/sleep 50)

      (ml/transition! latch :advance)
      (is (= :woken (deref result 500 :not-delivered)))))

  (testing "transition wakes waiters for earlier states too"
    (let [make-latch (ml/machine-latch-factory task-machine)
          latch      (make-latch)
          started    (CountDownLatch. 1)
          result     (promise)]

      ;; Wait for :running, but we'll cancel directly to :settling
      (run-virtual
        (fn []
          (CountDownLatch/.countDown started)
          (ml/await latch :running)
          (deliver result (ml/get-state latch))))

      (CountDownLatch/.await started 1 TimeUnit/SECONDS)
      (Thread/sleep 50)

      ;; Cancel jumps past :running to :settling
      (ml/transition! latch :cancel)
      ;; Waiter should wake because :settling >= :running in order
      (is (= :settling (deref result 500 :not-delivered))))))
