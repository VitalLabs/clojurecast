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
      (memberAdded [_ e]
        (when (cluster/is-master?)
          (println :ADDED (.getMember e))))
      (memberAttributeChanged [_ e])
      (memberRemoved [_ e]
        (when (cluster/is-master?)
          (println :REMOVED (.getMember e)))))))

(defrecord Scheduler [^com.hazelcast.core.MultiMap jobs
                      ^ScheduledExecutorService exec
                      ^String registration-id]
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
          :registration-id (cluster/add-membership-listener listener)))))
  (stop [this]
    (if exec
      (do
        (.shutdown exec)
        (cluster/remove-membership-listener registration-id)
        (assoc this :jobs nil :exec nil :registration-id nil))
      this)))

(defmulti schedule (juxt :job/type (comp class :job/schedule)))

(defmulti unschedule (juxt :job/type (comp class :job/schedule)))

(defmulti reschedule (juxt :job/type (comp class :job/schedule)))

(defmulti run (comp (juxt :job/type :job/state) #(.get %)))

