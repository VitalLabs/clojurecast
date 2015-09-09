(ns clojurecast.scheduler
  (:require [clojurecast.core :as cc]
            [com.stuartsierra.component :as com])
  (:import [java.util.concurrent Executors ScheduledExecutorService]))

(defrecord Scheduler [jobs ^ScheduledExecutorService exec]
  com/Lifecycle
  (start [this]
    (if exec
      this
      (let [jobs (cc/multi-map "scheduler/jobs")
            exec (Executors/newSingleThreadScheduledExecutor)]
        (assoc this :exec exec))))
  (stop [this]
    (if exec
      (do
        (.shutdown exec)
        (assoc this :exec nil))
      this)))
