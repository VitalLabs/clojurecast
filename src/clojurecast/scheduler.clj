(ns clojurecast.scheduler
  (:require [clojurecast.core :as cc]
            [clojurecast.cluster :as cluster]
            [clojurecast.component :as com]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl])
  (:import [java.util.concurrent DelayQueue Executors ScheduledExecutorService]
           [java.util.concurrent ScheduledFuture ScheduledThreadPoolExecutor]
           [com.hazelcast.core Cluster MembershipListener EntryListener]
           [com.hazelcast.core MessageListener]
           [com.hazelcast.map.listener MapListener]
           [com.hazelcast.map.listener EntryAddedListener EntryRemovedListener]
           [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

(def ^:dynamic *job*)

(def ^:dynamic *scheduler*)

(defn ^com.hazelcast.core.IMap cluster-jobs
  []
  (cc/distributed-map "scheduler/jobs"))

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
  (.put (cluster-jobs)
        (cluster/local-member-uuid)
        (:job/id job)))

(defn unschedule
  [job]
  (let [job-ref (cc/atomic-reference (:job/id job))]
    (when (:job/topic-bus (.get job-ref))
      (.removeMessageListener (cc/reliable-topic (:job/id job))
                              (:job/topic-bus (.get job-ref)))))
  (.remove (cluster-jobs)
           (cluster/local-member-uuid)
           (:job/id job)))

(defmethod run [:job/t :job.state/pausing]
  [^com.hazelcast.core.IAtomicReference job-ref]
  (let [job (.get job-ref)]
    (.remove (cluster-jobs) (:job/id job))
    (assoc job
      :job/state :job.state/paused
      :job/timeout 0)))

(defmethod run [:job/t :job.state/complete]
  [^com.hazelcast.core.IAtomicReference job-ref]
  (let [job (.get job-ref)]
    (.destroy job-ref)
    job))

(defmethod run :default
  [^com.hazelcast.core.IAtomicReference job-ref]
  (assoc (.get job-ref)
    :job/state :job.state/pausing
    :job/timeout 0))

(defn get-task
  [job-id]
  {:pre [*scheduler*]}
  (get (:tasks *scheduler*) job-id))

(defn cancel-task
  [job-id]
  {:pre [*scheduler*]}
  (when-let [task (get-task job-id)]
    (async/close! task)))

(defn remove-task
  [job-id]
  {:pre [*scheduler*]}
  (when-let [task (get-task job-id)]
    (cancel-task task)
    (swap! (:tasks *scheduler*) dissoc job-id)
    task))

(defn create-task
  [job-id]
  {:pre [*scheduler*]}
  (cancel-task job-id)
  (let [task (async/promise-chan)]
    (swap! (:tasks *scheduler*) assoc job-id task)
    task))

(defn resume
  [job-id]
  {:pre [*scheduler*]}
  (when-let [task (get-task job-id)]
    (async/put! task :resume)))

(defn- run-job
  [job-id]
  (let [job-ref (cc/atomic-reference job-id)
        init-task (create-task job-id)
        init-timeout (:job/timeout (.get job-ref))]
    (letfn [(run* []
              (try
                (run (.get job-ref))
                (catch Throwable e
                  (let [job (.get job-ref)]
                    (.remove (cluster-jobs) (:job/id job))
                    (assoc job
                           :job/state :job.state/failed
                           :job/error e
                           :job/timeout 0)))))]
      (async/go-loop [task init-task
                      timeout-ms init-timeout]
        (let [[val ch] (async/alts! [task (async/timeout timeout-ms)])
              ^com.hazelcast.core.IAtomicReference job-ref job-ref]
          (if (= task ch)
            (cond
              (= val :resume) (recur (create-task job-id)
                                     (:job/timeout (.get job-ref)))
              (= val :stop) nil
              :else (.destroy job-ref))
            (let [oldval (.get job-ref)
                  newval (run* oldval)]
              (.set job-ref newval)
              (if (#{:job.state/paused :job.state/failed} (:job/state newval))
                (async/<! task)
                (async/>! task newval))
              (if (= (:job/state newval) :job.state/terminated)
                (.destroy job-ref)
                (recur (create-task job-id)
                       (:job/timeout (.get job-ref)))))))))))

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

;; Distributed Job Map (each node owns entries according to partitioning strategy)
;; LocalEntryListener (informs member when it gains ownership of an entry)
;; - Add a local message listener to the entry topic
;; - Schedule the next task wake up time
;; DelayQueue (locally managed 'next task' queue)
;; ScheduledExecutor to wake up at 'next task' time

;; Update atomic refs to agents to improve job API surface

;; Jobs semantics
;; - Schedule
;; - Reschedule
;; - Unschedule

(defn- ^MapListener job-entry-listener
  [tasks]
  (reify
    EntryAddedListener
    (entryAdded [_ e]
      (let [job-id (.getValue e)]
        (add-job-listener job-id)
        (run-job job-id exec tasks)))
    
    EntryRemovedListener
    (entryRemoved [_ e]
      (let [job-id (.getOldValue e)]
        (remove-job-listener job-id)
        (when-let [^ScheduledFuture task (get tasks job-id)]
          (.cancel task false)
          (swap! tasks dissoc job-id))))))

(defrecord Scheduler [^String listener-id tasks]
  com/Component
  (initialized? [_] true)
  (started? [_] (boolean tasks))
  (migrate? [_] false)
  (-init [this] this)
  (-start [this]
    (let [jobs (cluster-jobs)
          listener (job-entry-listener tasks)
          this (assoc this
                      :listener-id (.addLocalEntryListener jobs listener)
                      :tasks (atom {}))]
      (if (thread-bound? #'*scheduler*)
        (set! *scheduler* this)
        (.bindRoot #'*scheduler* this))
      this))
  (-stop [this]
    (.removeEntryListener (cluster-jobs) listener-id)
    (if (thread-bound? #'*scheduler*)
      (set! *scheduler* nil)
      (.bindRoot #'*scheduler* nil))
    (doseq [[job-id ch] @tasks]
      (async/>!! ch :stop))
    (assoc this :tasks nil))
  (-migrate [this] this))

(defn reschedule
  [job]
  (let []
    (.remove (cluster-jobs)
             (cluster/local-member-uuid)
             (:job/id job))
    (.put (cluster-jobs)
          (cluster/local-member-uuid)
          (:job/id job))))

