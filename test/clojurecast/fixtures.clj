(ns clojurecast.fixtures
  (:use clojure.test)
  (:require [com.stuartsierra.component :as com]
            [clojurecast.core :as cc]
            [clojurecast.scheduler :as scheduler]
            [clojure.tools.logging :as log]))

(defonce ^{:doc "Mock system used for testing."}
  system
  nil)

(def ^:private ^:dynamic *job-history*
  [])

(defn- ^:dynamic *mock-history-fn*
  [action job-state]
  (set! *job-history* (conj *job-history* [action job-state])))

(def ^:private node-config
  {:history-fn `*mock-history-fn*})

(defn- make-system
  []
  (com/system-map

   ;; Mock System A
   :node1 (cc/map->Node {})
   :scheduler1 (com/using (scheduler/map->Scheduler {:config node-config})
                          {:node :node1})

   ;; Mock System B
   :node2 (com/using (cc/map->Node {}) [:node1])
   :scheduler2 (com/using (scheduler/map->Scheduler {:config node-config})
                          {:node :node2})))

(defn with-mock-system
  "Establishes the global mock system used for testing."
  [call-next-method]
  (binding [*mock-history-fn* *mock-history-fn*
            *job-history* []]
    ;; Start system once and only once for all tests.
    (alter-var-root #'system (fn [_] (make-system)))
    (alter-var-root #'system com/start-system)
    (is system "System is unavailable.")
    (call-next-method)
    (alter-var-root #'system com/stop-system)
    (alter-var-root #'system (constantly nil))
    (is (nil? system) "System is still available after shutdown.")
    (alter-var-root #'*job-history* (constantly *job-history*))))

