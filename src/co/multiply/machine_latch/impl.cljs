(ns co.multiply.machine-latch.impl
  "CLJS implementation of MachineLatch."
  (:refer-clojure :exclude [await]))


;; # Protocol
;; ################################################################################
(defprotocol IMachineLatch
  (-get-state [this]
    "Returns the current state keyword.")
  (-at-or-past? [this target-state]
    "Returns true if current state >= target-state in declared order.")
  (-at-or-past-idx? [this target-idx]
    "Returns true if current state index >= target-idx.")
  (-transition! [this action]
    "Attempt to perform action. Returns true if succeeded, false otherwise.")
  (-await [this target-state]
    "Returns a Promise that resolves to true when state >= target-state.")
  (-await-millis [this target-state timeout-ms]
    "Returns a Promise that resolves to true when state >= target-state, or false on timeout."))


;; # Latch type
;; ################################################################################
(deftype MachineLatch [idx->state state->idx transition-fn
                       ^:mutable current-idx
                       ^:mutable waiters]
  IMachineLatch
  (-get-state [_]
    (idx->state current-idx))

  (-at-or-past-idx? [_ target-idx]
    (>= current-idx target-idx))

  (-at-or-past? [this target-state]
    (let [target-idx (state->idx target-state)]
      (assert (some? target-idx) (str "Unknown state: " target-state))
      (-at-or-past-idx? this target-idx)))

  (-transition! [_ action]
    (let [to-idx (transition-fn action current-idx)]
      (if (some? to-idx)
        (do
          (set! current-idx to-idx)
          ;; Wake waiters whose target has been reached
          (let [wake #js []
                keep #js []]
            (dotimes [i (.-length waiters)]
              (let [w (aget waiters i)]
                (if (<= (unchecked-get w "target-idx") to-idx)
                  (.push wake w)
                  (.push keep w))))
            (set! waiters keep)
            ;; Resolve after updating waiters to avoid reentrancy issues
            (dotimes [i (.-length wake)]
              ((unchecked-get (aget wake i) "resolve") true)))
          true)
        false)))

  (-await [this target-state]
    (let [target-idx (state->idx target-state)]
      (assert (some? target-idx) (str "Unknown state: " target-state))
      (if (>= current-idx target-idx)
        (js/Promise.resolve true)
        (js/Promise.
          (fn [resolve _]
            (.push waiters #js {"target-idx" target-idx "resolve" resolve}))))))

  (-await-millis [this target-state timeout-ms]
    (let [target-idx (state->idx target-state)]
      (assert (some? target-idx) (str "Unknown state: " target-state))
      (if (>= current-idx target-idx)
        (js/Promise.resolve true)
        (js/Promise.race
          #js [(-await this target-state)
               (js/Promise.
                 (fn [resolve _]
                   (js/setTimeout #(resolve false) timeout-ms)))])))))


;; # Constructor
;; ################################################################################
(defn make-latch
  "Create a new MachineLatch instance."
  [idx->state state->idx transition-fn]
  (MachineLatch. idx->state state->idx transition-fn 0 #js []))


