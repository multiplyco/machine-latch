(ns co.multiply.machine-latch.common
  "Shared compile-time helpers for machine-latch validation and compilation."
  (:require [clojure.set :as set]))


(defn terminal-states
  "Returns states that appear as destinations but never as sources.
   A valid machine has exactly one terminal state."
  [transitions]
  (let [all-froms (into #{} (mapcat keys) (vals transitions))
        all-tos   (into #{} (mapcat vals) (vals transitions))]
    (set/difference all-tos all-froms)))


(defn- throw-illegal-argument
  [msg]
  #?(:clj  (throw (IllegalArgumentException. ^String msg))
     :cljs (throw (js/Error. msg))))


(defn validate-machine!
  "Validates machine spec. Throws on invalid spec."
  [{:keys [states transitions]}]
  (let [state-set  (set states)
        state->idx (into {} (map-indexed (fn [i s] [s i])) states)
        terminals  (terminal-states transitions)]

    ;; Must have exactly one terminal state (catches dead ends too)
    (when (not= 1 (count terminals))
      (throw-illegal-argument (str "Machine must have exactly one terminal state, found: " terminals)))

    ;; All states in transitions must be declared
    (doseq [[action from->to] transitions
            [from to] from->to]
      (when-not (contains? state-set from)
        (throw-illegal-argument (str "Action " action " references undeclared from-state: " from)))
      (when-not (contains? state-set to)
        (throw-illegal-argument (str "Action " action " references undeclared to-state: " to)))

      ;; Transitions must go forward
      (when-not (< (state->idx from) (state->idx to))
        (throw-illegal-argument (str "Action " action " has backward transition: " from " -> " to))))))


(defn compile-transition-fn
  "Compile transitions map to a nested case function.
   Input: {action-kw {from-idx to-idx, ...}, ...}
   Output: (fn [action from-idx] (case action ...))"
  [transitions state->idx]
  (let [action-sym (gensym "action")
        from-sym   (gensym "from")]
    `(fn [~action-sym ~from-sym]
       (case ~action-sym
         ~@(mapcat (fn [[action action-transitions]]
                     [action `(case (unchecked-int ~from-sym)
                                ~@(into [] (comp cat (map state->idx)) action-transitions)
                                nil)])
             transitions)
         nil))))


