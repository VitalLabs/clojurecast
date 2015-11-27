(ns clojurecast.scheduler
  (:require [clojurecast.core :as cc]
            [clojurecast.cluster :as cluster]
            [clojurecast.component :as com])
  (:import [java.util.concurrent Executors ScheduledExecutorService]
           [java.util.concurrent ScheduledFuture ScheduledThreadPoolExecutor]
           [com.hazelcast.core Cluster MembershipListener EntryListener]
           [com.hazelcast.core MessageListener]
           [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

(def ^:dynamic *jobs-name* "scheduler/jobs")

(def ^:dynamic *test-mode* false)

(def ^:dynamic *job*)

(declare reschedule)

(defmulti run (comp (juxt :job/type :job/state)
                    #(.get ^com.hazelcast.core.IAtomicReference %)))

(defmulti handle-message (fn [^com.hazelcast.core.IAtomicReference job-ref
                              message]
                           [(:job/type (.get job-ref))
                            (:event/type message)]))

(defmethod handle-message :default
  [^com.hazelcast.core.IAtomicReference job-ref message]
  (assoc (.get job-ref)
    :job/state :job.state/running
    :job/timeout 0))

(defmethod handle-message [:job/t :job/touch]
  [^com.hazelcast.core.IAtomicReference job-ref message]
  (assoc (.get job-ref)
    :job/state :job.state/running
    :job/timeout 0))

(defn- job-message-listener
  [job-id]
  (reify MessageListener
    (onMessage [_ message]
      (let [job-ref (cc/atomic-reference job-id)
            job (handle-message job-ref (.getMessageObject message))]
        (.set job-ref job)
        (reschedule job)))))

(defn schedule
  [job]
  (.set (cc/atomic-reference (:job/id job))
        (assoc job
               :job/timeout (:job/timeout job 0)
               :job/state (:job/state job :job.state/running)))
  (.put (cc/multi-map *jobs-name*)
        (cluster/local-member-uuid)
        (:job/id job)))

(defn unschedule
  [job]
  (let [job-ref (cc/atomic-reference (:job/id job))]
    (when (:job/topic-bus (.get job-ref))
      (.removeMessageListener (cc/reliable-topic (:job/id job))
                              (:job/topic-bus (.get job-ref)))))
  (.remove (cc/multi-map *jobs-name*)
           (cluster/local-member-uuid)
           (:job/id job)))

(defmethod run [:job/t :job.state/pausing]
  [^com.hazelcast.core.IAtomicReference job-ref]
  (let [job (.get job-ref)]
    (.remove (cc/multi-map *jobs-name*)
             (cluster/local-member-uuid)
             (:job/id job))
    (assoc job
      :job/state :job.state/paused
      :job/timeout 0)))

(defmethod run :default
  [^com.hazelcast.core.IAtomicReference job-ref]
  (assoc (.get job-ref)
    :job/state :job.state/pausing
    :job/timeout 0))

(defn- move-job [jobs job-id old-member-id new-member-id]
  ;; NOTE: Do this in an HC transaction? This can lose IDs in rare circumstances
  (.remove jobs old-member-id job-id)
  (.put jobs new-member-id job-id))

(defn- scheduler-membership-listener
  []
  (let [jobs (cc/multi-map *jobs-name*)]
    (reify MembershipListener
      (memberAdded [_ e]
        ;; Re-balance tasks when memberAdded
        (when (cluster/is-master?)
          (let [new-member (.getMember e)
                new-id (.getUuid new-member)
                member-ids (map #(.getUuid ^com.hazelcast.core.Member %)
                                (cluster/members))
                all-jobs (.values jobs)
                target-count (/ (count all-jobs) (count member-ids))]
            (doseq [member-id member-ids]
              (let [part (.get jobs member-id)
                    move-count (- (count part) target-count)]
                (when (> move-count 0)
                  (doseq [job-id (take move-count part)]
                    (move-job jobs job-id member-id new-id))))))))
      (memberAttributeChanged [_ e])
      (memberRemoved [_ e]
        ;; Distribute tasks when member removed
        (when (cluster/is-master?)
          (let [removed-member (.getMember e)
                removed-id (.getUuid removed-member)
                outstanding (.get jobs removed-id)]
            (when (seq outstanding)              
              (loop [members (map #(.getUuid ^com.hazelcast.core.Member %)
                                  (cluster/members))
                     parts (partition-all (/ (count outstanding) (count members))
                                          outstanding)]
                (when (seq members)
                  (let [member-id (first members)
                        job-uuids (first parts)]
                    (doseq [uuid job-uuids]
                      (move-job jobs uuid removed-id member-id))
                    (recur (next members) (next jobs))))))))))))

(defn- ^Callable job-callable
  [^com.hazelcast.core.IAtomicReference job-ref]
  (fn []
    (try
      (binding [*job* job-ref]
        (run job-ref))
      (catch Throwable e
        (let [job (.get job-ref)]
          (.remove (cc/multi-map *jobs-name*)
                   (cluster/local-member-uuid)
                   (:job/id job))
          (assoc job
            :job/state :job.state/failed
            :job/error e
            :job/timeout 0))))))

(defn- job-timeout
  ^long [^com.hazelcast.core.IAtomicReference job-ref]
  (if *test-mode*
    0
    (:job/timeout (.get job-ref))))

(defn- run-job
  [job-id ^ScheduledThreadPoolExecutor exec tasks]
  (let [job-ref (cc/atomic-reference job-id)
        ^ScheduledFuture scheduled-future
        (.schedule exec
                   (job-callable job-ref)
                   (job-timeout job-ref)
                   TimeUnit/MILLISECONDS)]
    (swap! tasks assoc job-id scheduled-future)
    (future
      (swap! tasks dissoc job-id)
      (when-let [job @scheduled-future]
        (.set job-ref job)
        (cond
          (.isCancelled scheduled-future) :cancel
          (= (:job/state job) :job.state/paused) :pause
          (= (:job/state job) :job.state/failed) :failed
          :else (run-job job-id exec tasks))))))

(defn remove-job-listener
  "Remove the current listener from a job, e.g. migrating
   job to another member"
  [job-id]
  (let [job-ref (cc/atomic-reference job-id)
        job (.get job-ref)]
    (when-let [listener (:job/topic-bus job)]
      (.removeMessageListener (cc/reliable-topic job-id)
                              listener)
      (.set job-ref (dissoc job :job/topic-bus)))))

(defn add-job-listener [job-id]
  "Add the job topic listener for the current member"
  (let [job-ref (cc/atomic-reference job-id)
        job (.get job-ref)]
    (.set job-ref
          (assoc job
                 :job/topic-bus (.addMessageListener
                                 (cc/reliable-topic job-id)
                                 (job-message-listener job-id))))))

(defn- job-entry-listener
  [exec tasks]
  (reify EntryListener
    (entryAdded [_ e]
      (let [job-id (.getValue e)]
        (add-job-listener job-id)
        (run-job job-id exec tasks)))
    (entryRemoved [_ e]
      (let [job-id (.getOldValue e)]
        (remove-job-listener job-id)
        (when-let [^ScheduledFuture task (get tasks job-id)]
          (.cancel task false)
          (swap! tasks dissoc job-id))))))

(defn register-local-jobs
  "Register every job assigned to this node, should be idempotnet
   but involves some churn on message listeners and "
  [jobs]
  (doseq [job-id (.get jobs (cluster/local-member-uuid))]
    (reschedule {:job/id job-id})))

(defrecord Scheduler [^com.hazelcast.core.MultiMap jobs
                      ^ScheduledExecutorService exec
                      ^String membership-listener-id
                      ^String entry-listener-id
                      tasks]
  com/Component
  (initialized? [_] true)
  (started? [_] (boolean exec))
  (migrate? [_] false)
  (-init [this] this)
  (-start [this]
    (let [jobs (cc/multi-map *jobs-name*)
          exec (doto ^ScheduledThreadPoolExecutor
                   (Executors/newScheduledThreadPool 1)
                 (.setRemoveOnCancelPolicy true))
          tasks (atom {})
          listener (scheduler-membership-listener)
          job-listener (job-entry-listener exec tasks)
          component (assoc this
                           :jobs jobs
                           :exec exec
                           :membership-listener-id (cluster/add-membership-listener listener)
                           :entry-listener-id (.addEntryListener jobs job-listener
                                                                 (cluster/local-member-uuid)
                                                                 true)
                           :tasks tasks)]
      ;; Add jobs if master in cluster has already moved them while my
      ;; listener was not yet scheduled
      (register-local-jobs jobs)
      component))
  (-stop [this]
    (.shutdown exec)
    (.removeEntryListener jobs entry-listener-id)
    (cluster/remove-membership-listener membership-listener-id)
    (assoc this :jobs nil :exec nil))
  (-migrate [this] this))

(defn reschedule
  [job]
  (let []
    (.remove (cc/multi-map *jobs-name*)
             (cluster/local-member-uuid)
             (:job/id job))
    (.put (cc/multi-map *jobs-name*)
          (cluster/local-member-uuid)
          (:job/id job))))

