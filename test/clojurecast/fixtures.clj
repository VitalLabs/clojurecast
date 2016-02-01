(ns clojurecast.fixtures
  (:use clojure.test)
  (:require [com.stuartsierra.component :as com]
            [clojurecast.core :as cc]
            [clojurecast.scheduler :as scheduler]
            [clojure.tools.logging :as log]))

(def system
  "Mock system used for testing."
  nil)

(defn- ^:dynamic *mock-history-fn*
  [action job-state]
  (log/info :history-fn action job-state))

(def ^:private node-config
  {:history-fn `*mock-history-fn*})

(defn- make-system
  []
  (com/system-map

   ;; Mock System A
   :node1 (cc/map->Node {})
   :scheduler1 (com/using (scheduler/map->Scheduler {:config node-config})
                          [:node1])

   ;; Mock System B
   :node2 (com/using (cc/map->Node {}) [:node1])
   :scheduler2 (com/using (scheduler/map->Scheduler {:config node-config})
                          [:node2])))

(defn with-mock-system
  "Establishes the global mock system used for testing."
  [call-next-method]
  (binding [*mock-history-fn* *mock-history-fn*]
    ;; Start system once and only once for all tests.
    (when-not system      
      (alter-var-root #'system (fn [_] (make-system)))
      (alter-var-root #'system com/start-system))
    (call-next-method)))
