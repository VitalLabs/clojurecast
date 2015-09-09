(ns user
  (:require [clojure.tools.namespace.repl :as repl :refer [refresh refresh-all]]
            [com.stuartsierra.component :as com]
            [clojurecast.core :as cc]
            [clojurecast.scheduler :as scheduler]))

(def system nil)

(defn init
  "Creates and initializes the system under development in the Var
  #'system."
  []
  (.bindRoot #'system (com/system-map
                       :node (cc/map->Node {})
                       :scheduler (com/using (scheduler/map->Scheduler {})
                                             [:node]))))

(defn start
  "Starts the system running, updates the Var #'system."
  []
  (alter-var-root #'system com/start-system))

(defn stop
  "Stops the system if it is currently running, updates the Var
  #'system."
  []
  (alter-var-root #'system com/stop-system))

(defn go
  "Initializes and starts the system running."
  []
  (init)
  (start)
  :ready)

(defn reset
  "Stops the system, reloads modified source files, and restarts it."
  []
  (stop)
  (refresh :after 'user/go))
