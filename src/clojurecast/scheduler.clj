(ns clojurecast.scheduler
  (:require [clojurecast.core :as cc]
            [clojurecast.cluster :as cluster]
            [com.stuartsierra.component :as com])
  (:import [java.util.concurrent Executors ScheduledExecutorService]
           [com.hazelcast.core Cluster MembershipListener]))

(defn- scheduler-membership-listener
  []
  (let [jobs (cc/multi-map "scheduler/jobs")]
    (reify MembershipListener
      (memberAdded [_ e])
      (memberAttributeChanged [_ e])
      (memberRemoved [_ e]))))

(defrecord Scheduler [^com.hazelcast.core.MultiMap jobs
                      ^ScheduledExecutorService exec
                      ^String listener-id]
  com/Lifecycle
  (start [this]
    (if exec
      this
      (let [jobs (cc/multi-map "scheduler/jobs")
            exec (Executors/newSingleThreadScheduledExecutor)
            listener (scheduler-membership-listener)]
        (cluster/add-membership-listener listener :id "scheduler/jobs")
        (assoc this
          :jobs jobs
          :exec exec))))
  (stop [this]
    (if exec
      (do
        (.shutdown exec)
        (assoc this :jobs nil :exec nil))
      this)))
