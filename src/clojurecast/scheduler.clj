(ns clojurecast.scheduler
  (:require [clojurecast.core :as cc]
            [com.stuartsierra.component :as com])
  (:import [java.util.concurrent Executors ScheduledExecutorService]
           [com.hazelcast.core Cluster MembershipListener]))

(defn- scheduler-membership-listener
  []
  (let [jobs (cc/multi-map "scheduler/jobs")]
    (reify MembershipListener
      (memberAdded [_ e]
        (println e))
      (memberAttributeChanged [_ e]
        (println e))
      (memberRemoved [_ e]
        (println e)))))

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
        (assoc this
          :jobs jobs
          :exec exec
          :listener-id (.addMembershipListener (cc/cluster) listener)))))
  (stop [this]
    (if exec
      (do
        (.shutdown exec)
        (.removeMembershipListener (cc/cluster) listener-id)
        (assoc this :jobs nil :exec nil))
      this)))
