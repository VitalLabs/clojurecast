(ns clojurecast.fixtures
  (:use clojure.test)
  (:require [com.stuartsierra.component :as com]
            [clojurecast.core :as cc]
            [clojurecast.scheduler :as scheduler]
            [clojure.tools.logging :as log]))

(defonce ^{:doc "Mock system used for testing."}
  system
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
    (is system "System is unavailable.")
    (call-next-method)))

(defmacro with-node
  "Run the body with *instance* bound to the given node instance."
  [node & body]
  `(binding [cc/*instance* (:instance ~node)]
     ~@body))

(defmacro with-scheduler
  "Run the body with *scheduler* bound to the given scheduler."
  [scheduler & body]
  `(binding [scheduler/*scheduler* ~scheduler]
     ~@body))

(defmacro with-system1
  [& body]
  `(with-node (:node1 system)
     (with-scheduler (:scheduler1 system)
       ~@body)))

(defmacro with-system2
  [& body]
  `(with-node (:node2 system)
     (with-scheduler (:scheduler2 system)
       ~@body)))
