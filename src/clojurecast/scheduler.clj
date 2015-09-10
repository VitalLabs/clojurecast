(ns clojurecast.scheduler
  (:require [clojurecast.core :as cc]
            [clojurecast.cluster :as cluster]
            [com.stuartsierra.component :as com])
  (:import [java.util.concurrent Executors ScheduledExecutorService]
           [com.hazelcast.core Cluster MembershipListener EntryListener]))

(defn- scheduler-membership-listener
  []
  (let [jobs (cc/multi-map "scheduler/jobs")]
    (reify MembershipListener
      (memberAdded [_ e])
      (memberAttributeChanged [_ e])
      (memberRemoved [_ e]
        (when (cluster/is-master?)
          (let [removed-member (.getMember e)
                outstanding-jobs (.get jobs (.getUuid removed-member))]
            (when (seq outstanding-jobs)
              (println outstanding-jobs))))))))

(defn- job-entry-listener
  [exec]
  (reify EntryListener
    (entryAdded [_ e]
      (let [job-ref (cc/atomic-reference (.getValue e))]
        (println :SCHEDULE job-ref)))
    (entryRemoved [_ e]
      (let [job-ref (cc/atomic-reference (.getOldValue e))]
        (println :UNSCHEDULE job-ref)))))

(defrecord Scheduler [^com.hazelcast.core.MultiMap jobs
                      ^ScheduledExecutorService exec
                      ^String membership-listener-id
                      ^String entry-listener-id]
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
          :membership-listener-id (cluster/add-membership-listener listener)
          :entry-listener-id (.addEntryListener jobs (job-entry-listener exec)
                                                (cluster/local-member-uuid)
                                                true)))))
  (stop [this]
    (if exec
      (do
        (.shutdown exec)
        (.removeEntryListener jobs entry-listener-id)
        (cluster/remove-membership-listener membership-listener-id)
        (assoc this :jobs nil :exec nil))
      this)))

(defn schedule
  [job]
  (.set (cc/atomic-reference (:job/id job)) job)
  (.put (cc/multi-map "scheduler/jobs")
        (cluster/local-member-uuid)
        (:job/id job)))

(defn unschedule
  [job]
  (.remove (cc/multi-map "scheduler/jobs")
           (cluster/local-member-uuid)
           (:job/id job)))

(defn reschedule
  [job]
  (unschedule job)
  (schedule job))

(defmulti run (comp (juxt :job/type :job/state) #(.get %)))

