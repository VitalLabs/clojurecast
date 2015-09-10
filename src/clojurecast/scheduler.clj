(ns clojurecast.scheduler
  (:require [clojurecast.core :as cc]
            [clojurecast.cluster :as cluster]
            [com.stuartsierra.component :as com])
  (:import [java.util.concurrent Executors ScheduledExecutorService]
           [java.util.concurrent ScheduledThreadPoolExecutor]
           [com.hazelcast.core Cluster MembershipListener EntryListener]
           [com.hazelcast.core MessageListener]
           [java.util.concurrent TimeUnit]))

(defmulti run (comp (juxt :job/type :job/state) #(.get %)))

(defmulti handle-message :event/type)

(defn- job-message-listener
  []
  (reify MessageListener
    (onMessage [_ message]
      (handle-message message))))

(defn schedule
  [job]
  (.set (cc/atomic-reference (:job/id job))
        (assoc job
          :job/topic-bus (.addMessageListener (cc/reliable-topic (:job/id job))
                                              (job-message-listener))))
  (.put (cc/multi-map "scheduler/jobs")
        (cluster/local-member-uuid)
        (:job/id job)))

(defn unschedule
  [job]
  (let [job-ref (cc/atomic-reference (:job/id job))]
    (.removeMessageListener (cc/reliable-topic (:job/id job))
                            (:job/topic-bus (.get job-ref))))
  (.remove (cc/multi-map "scheduler/jobs")
           (cluster/local-member-uuid)
           (:job/id job)))

(defmethod run [:job/t :job.state/waiting]
  [job-ref]
  (let [job (.get job-ref)]
    (unschedule job)
    job))

(defmethod run :default
  [job-ref]
  (assoc (.get job-ref) :job/state :job.state/waiting))

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
                        job-uuids (first parts)]
                    (doseq [uuid job-uuids]
                      (.put jobs member uuid))
                    (recur (next members) (next jobs))))))))))))

(defn- run-job
  [job-id exec tasks]
  (let [job-ref (cc/atomic-reference job-id)
        scheduled-future (.schedule exec
                                    ^Callable (fn [] (run job-ref))
                                    (:job/timeout (.get job-ref))
                                    TimeUnit/MILLISECONDS)]
    (swap! tasks assoc job-id scheduled-future)
    (future
      (when-let [job @scheduled-future]
        (.set job-ref job)
        (when-not (or (.isCancelled scheduled-future)
                      (= (:job/state job) :job.state/waiting))
          (run-job job-id exec tasks))))))

(defn- job-entry-listener
  [exec tasks]
  (reify EntryListener
    (entryAdded [_ e]
      (run-job (.getValue e) exec tasks))
    (entryRemoved [_ e]
      (let [job-id (.getOldValue e)]
        (when-let [task (get tasks job-id)]
          (.cancel task false)
          (swap! tasks dissoc job-id))))))

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

(defn reschedule
  [job]
  (unschedule job)
  (schedule job))

;; (defmethod run [:job/tracker :continue-tracking]
;;   [job-ref]
;;   (let [job (.get job-ref)]
;;     (println job)
;;     (assoc job :job/state )))
