(ns clojurecast.scheduler
  (:require [clojurecast.core :as cc]
            [clojurecast.cluster :as cluster]
            [com.stuartsierra.component :as com])
  (:import [java.util.concurrent Executors ScheduledExecutorService]
           [java.util.concurrent ScheduledThreadPoolExecutor]
           [com.hazelcast.core Cluster MembershipListener EntryListener]
           [java.util.concurrent TimeUnit]))

(defmulti run (comp (juxt :job/type :job/state) #(.get %)))

(defmethod run :default [job])

(defn- scheduler-membership-listener
  []
  (let [jobs (cc/multi-map "scheduler/jobs")]
    (reify MembershipListener
      (memberAdded [_ e])
      (memberAttributeChanged [_ e])
      (memberRemoved [_ e]
        (when (cluster/is-master?)
          (let [removed-member (.getMember e)
                outstanding (.get jobs (.getUuid removed-member))]
            (when (seq outstanding)              
              (loop [members (map #(.getUuid %) (cluster/members))
                     parts (partition (count (cluster/members)) outstanding)]
                (when (seq members)
                  (let [member (first members)
                        jobs (first parts)]
                    (doseq [job jobs]
                      (.put (cc/multi-map "scheduler/jobs") member job))
                    (recur (next members) (next jobs))))))))))))

(defn- run-job
  [job-id exec tasks]
  (let [job-ref (cc/atomic-reference job-id)
        scheduled-future (.schedule exec
                                    (fn [] (run job-ref))
                                    (:job/timeout (.get job-ref))
                                    TimeUnit/MILLISECONDS)]
    (swap! tasks assoc job-id scheduled-future)
    (future
      @scheduled-future
      (when-not (.isCancelled scheduled-future)
        (run-job job-id exec tasks)))))

(defn- job-entry-listener
  [exec tasks]
  (reify EntryListener
    (entryAdded [_ e]
      (run-job (.getValue e) exec tasks))
    (entryRemoved [_ e]
      (let [job-id (.getOldValue e)]
        (.cancel (get tasks job-id) false)
        (swap! tasks dissoc job-id)))))

(defrecord Scheduler [^com.hazelcast.core.MultiMap jobs
                      ^ScheduledExecutorService exec
                      ^String membership-listener-id
                      ^String entry-listener-id
                      tasks]
  com/Lifecycle
  (start [this]
    (if exec
      this
      (let [jobs (cc/multi-map "scheduler/jobs")
            exec (doto (Executors/newScheduledThreadPool 1)
                   (.setRemoveOnCancelPolicy true))
            tasks (atom {})
            listener (scheduler-membership-listener)
            job-listener (job-entry-listener exec tasks)]
        (assoc this
          :jobs jobs
          :exec exec
          :membership-listener-id (cluster/add-membership-listener listener)
          :entry-listener-id (.addEntryListener jobs job-listener
                                                (cluster/local-member-uuid)
                                                true)
          :tasks tasks))))
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

(defmethod run [:job/tracker :continue-tracking]
  [job-ref]
  (let [job (.get job-ref)]
    (println job)))
