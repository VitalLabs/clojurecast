(ns clojurecast.scheduler
  (:require [clojurecast.core :as cc]
            [clojurecast.cluster :as cluster]
            [clojurecast.component :as com]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl])
  (:import [java.util.concurrent DelayQueue Executors ScheduledExecutorService]
           [java.util.concurrent ScheduledFuture ScheduledThreadPoolExecutor]
           [com.hazelcast.core Cluster MembershipListener EntryListener]
           [com.hazelcast.core MessageListener MigrationListener]
           [com.hazelcast.map.listener MapListener]
           [com.hazelcast.map.listener EntryAddedListener EntryRemovedListener]           
           [java.util.concurrent TimeUnit]))

;;
;; Clojurecast Scheduler v0.2
;;
;; The cluster maintains a distributed map of jobs (job-id ->
;; job-state). Job state should be a clojure map that contains only
;; serializable values.
;;
;; Each cluster member is assigned a subset of values in the map by
;; the HazelCast partitioner.  When values are added to the map, they
;; are 'scheduled' which includes being setup to listen to job topic
;; events and are immediately passed to a core.async go process, the
;; "run loop".  When they are removed from the map, they are unscheduled
;; and their associated state and run loop is cleaned up.
;;
;; The run loop will call the scheduler/run method with an initial
;; state and registered a control handler for updating the scheduler
;; run loop.
;;
;; Jobs that receive events via the job topic bus have a message
;; handler after which a :resume input is sent to the control loop.
;;
;; Scheduler Subsystem Terminology:
;; - Distributed Job Map (each member owns subset of entries according to partitioning strategy)
;; - Run Loop - a core.async go-loop that executes the workflow
;; - Controller ('ctrl') - a control channel that is used to manage the run loop
;; - LocalEntryListener
;;   - Called when member when owned key-value pairs are added/removed
;;   - Registers a listener to the job's topic
;;   - Starts the local core.async loop
;; - MigrationListener
;;   - Called when partitions are being migrated
;;   - Used to shut down old job controller and listener (if source is still up)
;;   - Creates new listeners and controller on new job when completed
;; - Scheduler
;;   - Contains local entry listener ID and the set of local controllers
;;


;;
;; Scheduler State and Dynamic Vars
;;

(set! *warn-on-reflection* true)

(def ^:dynamic *job*)

(defonce ^:dynamic *scheduler* nil)

(defn ^com.hazelcast.core.IMap cluster-jobs
  []
  (cc/distributed-map "scheduler/jobs"))

;;
;; JOB Management API
;;
;; Use to add, remove, and manage jobs across the cluster
;;

(declare get-job)

(defn schedule
  "Schedule a job for execution on the cluster.
   Job is a clojure map with a unique :job/id, it is an error to
  submit a job for a :job/id that is currently running. Provides
  standard defaults for :job/timeout and :job/state."
  [job]
  {:pre [(:job/id job) (nil? (get-job (:job/id job)))]}
  (.put (cluster-jobs)
        (:job/id job)
        (assoc job
               :job/state (:job/state job :job.state/running)
               :job/timeout (:job/timeout job 0))))
               
               
(defn unschedule
  "Remove the job from the cluster and clean up 
   any management state associated with it."
  [job-id]
  (.remove (cluster-jobs) job-id))

(defn send-to-job
  "Send a message to the job from any member"
  [job-id message]
  {:pre [(:event/type message)]}
  (.publish (cc/reliable-topic job-id) message))

(defn get-job
  "Get the current state of a running job from any member"
  [job-id]
  (.get (cluster-jobs) (str job-id)))

;;
;; JOB State API
;;
;; These two methods are implemented by jobs to handle events and
;; transition between job states.
;;

(defmulti run
  "The job run method dispatches on :job/type and :job/state.
   :job/type namespaces the specific job class and :job/state 
   is a value that indicates the current state."
  (juxt :job/type :job/state))

(defmulti handle-message
  "The handle-message deals with out-of-band events.  It can
   modify state and/or start the job running in response to
   the event.  It is also namespaced by :job/type"
  (fn [job message]
    [(:job/type job)
     (:event/type message)]))

;;
;; Default behavior for run and handle-message
;;
;;
;; Special job states:

;; - :job.state/init - A state to enter the first
;; time a job is run, default is to proceed directly to
;; :job.state/running

;; - :job.state/reinit - Jobs can implement this to
;; reset state after a migration or restart.  The previous state is in

;; :job/prior-state.  Default is to proceed to :job.state/running

;; - :job.state/running - All jobs implement this as the main entry
;; point

;; - :job.state/failed - The job crashed, storage and ctrl
;; retained but not timeout If you override, you can force a reset on
;; an internal failure to recover, or notify the admin of an error.
;; Job state is:

;; - :job/error - the error encountered

;; - :job/prior-state - the state the job was in when crashing if run

;; - :job/msg - the message passed if handle-message with the prior
;; tstate

;; - :job.state/paused - The job is paused, storage and ctrl
;; retianed but no timeout

;; - :job.state/terminated - Moving to this
;; state unschedules the job and reclaims storage and state.
;;

(defmethod run [:job/t :job.state/init]
  [job]
  (assoc job
         :job/state :job.state/running
         :job/timeout 0))

(defmethod run [:job/t :job.state/reinit]
  [job]
  (-> job
      (dissoc :job/prior-state)
      (assoc
       :job/state :job.state/running
       :job/timeout 0)))

(defmethod run [:job/t :job.state/failed]
  [job]
  job)
  
(defmethod run [:job/t :job.state/running]
  [job]
  (throw (ex-info "All jobs should implement run for :job.state/running" {})))

(defmethod run :default
  [job]
  (throw (ex-info "Unhandled state in run method" {})))


;; Special message types :event/type
;; - :job/touch - move the job into the running state

;; The touch event is the same as the default event
(defmethod handle-message [:job/t :job/touch]
  [job message]
  (assoc job
    :job/state :job.state/running
    :job/timeout 0))

;; The default message handler is a touch
(defmethod handle-message :default
  [job message]
  (assoc job
    :job/state :job.state/running
    :job/timeout 0))

;;
;; Job controller API (internal)
;;

;; NOTE: This is made internal because we don't want
;; external users to be too aware of the internal structure
;; and screw up things like job topic listeners, etc.

(defn- job-ctrl
  "Return the job control channel"
  [job-id]
  {:pre [*scheduler*]}
  (get @(:ctrls *scheduler*) (str job-id)))

(defn- cancel-ctrl
  "Stop the run loop"
  [job-id]
  {:pre [*scheduler*]}
  (when-let [ctrl (job-ctrl job-id)]
    (async/close! ctrl)))

(defn- remove-ctrl
  "Delete the run loop"
  [job-id]
  {:pre [*scheduler*]}
  (when-let [ctrl (job-ctrl job-id)]
    (cancel-ctrl ctrl)
    (swap! (:ctrls *scheduler*) dissoc (str job-id))
    ctrl))

(defn- create-ctrl
  "Create a new "
  [job-id]
  {:pre [*scheduler*]}
  (cancel-ctrl job-id)
  (let [ctrl (async/promise-chan)]
    (swap! (:ctrls *scheduler*) assoc (str job-id) ctrl)
    ctrl))

(defn- resume-ctrl
  [job-id]
  {:pre [*scheduler*]}
  (when-let [ctrl (job-ctrl job-id)]
    (async/put! ctrl :resume)))

(defn- run-job
  [job-id]
  (async/go-loop []
    (let [job (get-job job-id)
          ctrl (create-ctrl job-id)
          timeout-ms (:job/timeout job)
          [val ch] (async/alts! [ctrl (async/timeout timeout-ms)])]
      (if (= ctrl ch)
        (cond
          (= val :resume) (recur)
          (= val :stop) (unschedule job-id)
          :else (unschedule job-id))
        (let [newjob (try
                       (run job)
                       (catch Throwable e
                         (assoc job
                                :job/state :job.state/failed
                                :job/prior-state (:job/state job)
                                :job/error e
                                :job/timeout 0)))]
          (when newjob
            (.put (cluster-jobs) job-id newjob))
          (when (#{:job.state/paused :job.state/failed} (:job/state newjob))
            (async/<! ctrl)) ;; block on a channel event to proceed
          (if (= (:job/state newjob) :job.state/terminated)
            (unschedule job-id) ;; unschedule completely if terminated
            (recur)))))))


;;
;; Listening to the topic bus
;;

(defn- job-message-listener
  [job-id]
  (reify MessageListener
    (onMessage [_ message]
      (let [msg (.getMessageObject message)
            job (get-job job-id)
            newjob (try
                     (handle-message job msg)
                     (catch Throwable e
                       (assoc job
                              :job/state :job.state/failed
                              :job/msg msg
                              :job/error e
                              :job/timeout 0)))]
        (.put (cluster-jobs) job-id newjob)
        (resume-ctrl job-id)))))


(defn remove-job-listener
  "Remove the current listener from a job, e.g. migrating
   job to another member"
  [job-id]
  (let [job (get-job job-id)]
    (when-let [listener (:job/topic-bus job)]
      (.removeMessageListener (cc/reliable-topic job-id) listener)
      (.put (cluster-jobs) job-id (dissoc job :job/topic-bus)))))

(defn add-job-listener [job-id]
  "Add the job topic listener for the current member"
  (let [bus-id (.addMessageListener (cc/reliable-topic job-id)
                                    (job-message-listener job-id))
        job (assoc (get-job job-id) :job/topic-bus bus-id)]
    (.put (cluster-jobs) job-id job)))

;;
;; Migration events
;;

(defn local-partition-keys
  "Return the set of keys owned by this cluster
   belonging to the provided partition"
  [^com.hazelcast.core.IMap dmap partid]
  (let [psvc (cc/partition-service)]
    (filter #(= (.getPartition psvc %) partid)
            (seq (.localKeySet dmap)))))

(defn- ^MigrationListener migration-listener
  [ctrls]
  (reify
    MigrationListener
    (migrationStarted [_ e]
      ;; TODO: Record when migration started?
      )
    (migrationCompleted [_ e]
      (let [partid (.getPartitionId e)]
        ;; I'm no longer the owner
        (when (.localMember (.getOldOwner e))
          (doseq [job-id (local-partition-keys (cluster-jobs) partid)]
            (remove-job-listener job-id)
            (remove-ctrl job-id)))
        ;; I'm the new owner
        (when (.localMember (.getNewOwner e))
          (doseq [job-id (local-partition-keys (cluster-jobs) partid)]
            (let [job (get-job job-id)]
              (.put (cluster-jobs)
                    job-id
                    (assoc job
                           :job/state :job.state/reinit
                           :job/prior-state (:job/state job)
                           :job/timeout 0)))
            (run-job job-id)
            (add-job-listener job-id)))))
    (migrationFailed [_ e]
      ;; TODO: How to handle failed migrations
      )))

;;
;; Job Entry Events
;;

(defn- ^MapListener job-entry-listener
  [ctrls]
  (reify
    EntryAddedListener
    (entryAdded [_ e]
      (let [job-id (.getKey e)]
        (run-job job-id)
        (add-job-listener job-id)))
    EntryRemovedListener
    (entryRemoved [_ e]
      (let [job-id (.getKey e)]
        (remove-job-listener job-id)
        (remove-ctrl job-id)))))

;;
;; Scheduler Object
;;

(defrecord Scheduler [entry-id migration-id ctrls]
  com/Component
  (initialized? [_] true)
  (started? [_] (boolean ctrls))
  (migrate? [_] false)
  (-init [this] this)
  (-start [this]
    (let [jobs (cluster-jobs)
          part (cc/partition-service)
          eid (.addLocalEntryListener jobs (job-entry-listener ctrls))
          mid (.addMigrationListener part (migration-listener ctrls))
          this (assoc this
                      :entry-id eid
                      :migration-id mid
                      :ctrls (atom {}))]
      (if (thread-bound? #'*scheduler*)
        (set! *scheduler* this)
        (.bindRoot #'*scheduler* this))
      (doseq [job-id (seq (.localKeySet (cluster-jobs)))]
        (run-job job-id)
        (add-job-listener job-id))
      this))
  (-stop [this]
    (doseq [job-id (seq (.localKeySet (cluster-jobs)))]
      (unschedule job-id))
    (.removeEntryListener (cluster-jobs) entry-id)
    (.removeMigrationListener (cc/partition-service) migration-id)
    (if (thread-bound? #'*scheduler*)
      (set! *scheduler* nil)
      (.bindRoot #'*scheduler* nil))    
    (assoc this :ctrls nil :entry-id nil))
  (-migrate [this] this))
