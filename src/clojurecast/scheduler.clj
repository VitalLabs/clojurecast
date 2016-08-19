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
           [com.hazelcast.map.listener
            EntryAddedListener EntryUpdatedListener EntryRemovedListener]
           [java.util.concurrent TimeUnit]))

;;
;; # Clojurecast Scheduler v0.2
;;
;; The cluster maintains a distributed map of jobs (job-id ->
;; job-state). Job state should be a clojure map that contains only
;; serializable values.
;;
;; By contract, the cluster will maintain a single core.async go-loop
;; that handles calling the job multi-method 'run' on startup and
;; timeout events and message arrival events.  When the cluster
;; migrates data to/from a node due to a node outage or controlled
;; lifecycle (e.g. rolling upgrade), the async loop and topic handler
;; is migrated to a successor node.
;;
;; A CC client can maintain 
;;
;; NOTE: Due to the use of topics, messages to jobs that arrive
;; during cluster migration will be lost.
;;
;; ## Implementation
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
;; ## Scheduler Subsystem Terminology
;;
;; - Scheduler
;;   - Contains local entry listener ID and the set of local Controllers
;; - Distributed Job Map (each member owns subset of entries according to partitioning strategy)
;; - Run Loop - a core.async go-loop that executes the workflow
;; - Listener - a topic listener for this job to receive messages
;; - Controller ('ctrl') - a control channel that is used to manage the run loop
;; - LocalEntryListener
;;   - Called when member when owned key-value pairs are added/removed
;;   - Registers a listener to the job's topic
;;   - Starts the local core.async loop
;; - MigrationListener
;;   - Called when partitions are being migrated
;;   - Used to shut down Run Loop and Listener (if source is still up)
;;   - Creates new Run Loop and Listener on new cluster
;;


;;
;; Scheduler State and Dynamic Vars
;;

(set! *warn-on-reflection* true)

(def ^:dynamic *job*)

(defonce ^:dynamic *scheduler* nil)

(defmacro with-scheduler
  [scheduler & body]
  `(let [scheduler# ~scheduler]
     (binding [cc/*instance* (get-in scheduler# [:node :instance])
               *scheduler* scheduler#]
       ~@body)))

(defn- ^com.hazelcast.core.HazelcastInstance scheduler-instance
  "Private getter for the HazelcastInstance which instantiated *scheduler*."
  ([]
   {:pre [*scheduler*]}
   (scheduler-instance *scheduler*))
  ([scheduler]
   (get-in scheduler [:node :instance])))

(defn ^com.hazelcast.core.IMap cluster-jobs
  "Public getter for the cluster-wide job map."
  ([]
   {:pre [*scheduler*]}
   (cluster-jobs (scheduler-instance)))
  ([instance]
   (cc/distributed-map instance "cluster-jobs")))

;;
;; JOB Management API
;;
;; Use to add, remove, and manage jobs across the cluster
;;

(defn get-job
  "Public getter for the current state of a running job from any node."
  [job]
  {:pre [*scheduler*]}
  (cond
    (map? job)
    (.get (cluster-jobs) (str (:job/id job)))
    (or (string? job) (instance? java.util.UUID job))
    (.get (cluster-jobs) (str job))
    :else (throw (ex-info "Job is not one of [map|string|uuid]" {:job job}))))

(defn- put-job!
  "Private mutator for adding a job to the distributed cluster jobs map."
  [job]
  {:pre [*scheduler*]}
  (.put (cluster-jobs) (str (:job/id job)) (update job :job/id str)))

(defn- remove-job!
  "Private mutator for removing a job to the distributed cluster jobs map."
  [job]
  {:pre [*scheduler*]}
  (cond
    (map? job)
    (.remove (cluster-jobs) (str (:job/id job)))
    (or (string? job) (instance? java.util.UUID job))
    (.remove (cluster-jobs) (str job))
    :else (throw (ex-info "Job is not one of [map|string|uuid]" {:job job}))))

(defn schedule
  "Schedule a job for execution on the cluster.
   Job is a clojure map with a unique :job/id, it is an error to
  submit a job for a :job/id that is currently running. Provides
  standard defaults for :job/timeout and :job/state."
  [job]
  {:pre [(:job/id job) (nil? (get-job (:job/id job)))]}
  (put-job! (assoc job
                   :job/state (:job/state job :job.state/running)
                   :job/timeout (:job/timeout job 0))))

(defn reschedule
  "Support the job's reinitialization protocol outside a migration,
   for example restoring a job from some external storage"
  [job]
  {:pre [(:job/id job) (nil? (get-job (:job/id job)))]}
  (put-job! (assoc job
                   :job/state :job.state/reinit
                   :job/prior-state (:job/state job)
                   :job/timeout 0)))

(defn unschedule
  "Remove the job from the cluster and clean up 
   any management state associated with it."
  [job-id]
  (remove-job! job-id))

(defn send-to-job
  "Send a message to the job from any member"
  [job-id message]
  {:pre [(:event/type message)]}
  (.publish (cc/reliable-topic job-id) message))

;;
;; JOB Implementors API
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

;; :job.state/running - All jobs implement as a default entry
;; point for the state machine.

;; :job.state/init - A state to enter the first
;; time a job is run, default is to proceed directly to

;; :job.state/reinit - Jobs can implement this to
;; reset state after a migration or restart.  The previous state is in

;;     :job/prior-state.  Default is to proceed to :job.state/running

;; :job.state/running - All jobs implement this

;; :job.state/failed - The job crashed, storage and ctrl
;; retained but not timeout If you override, you can force a reset on
;; an internal failure to recover, or notify the admin of an error.
;; Job state is:

;;     :job/error - the error object encountered
;;     :job/prior-state - the state the job was in prior to the error
;;     :job/msg - message passed if handle-message caused the error

;; :job.state/paused - The job is paused, storage and ctrl
;; retianed but no timeout

;; :job.state/terminated - Moving to this
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
    nil))

(defn- detach-ctrl
  [job-id]
  (when-let [ctrl (job-ctrl job-id)]
    (async/put! ctrl :detach) ;; leave channel to GC
    (swap! (:ctrls *scheduler*) dissoc (str job-id))
    nil))

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

(defn- scheduler-history-fn
  "Returns the current history-fn set in config on the scheduler."
  []
  {:pre [*scheduler*]}
  (get-in *scheduler* [:config :history-fn]))

(defn- record-job-history
  "history-fn :: action -> job-state -> Nil"
  [action job-state]
  (when-let [cb (scheduler-history-fn)]
    (when job-state
      (cb action job-state))))

(defn- run-job
  [scheduler job-id]
  (with-scheduler scheduler
    (async/go-loop [newjob nil]
      (let [job (or newjob (get-job job-id))
            ctrl (create-ctrl job-id)
            timeout-ms (:job/timeout job)
            [^clojure.lang.Keyword val ch]
            (async/alts! [ctrl (async/timeout timeout-ms)])]
        (when newjob
          (put-job! newjob))
        (if (= ctrl ch)
          (cond
            (= val :resume) (recur)
            (= val :detach) nil
            (= val :stop) (unschedule job-id)
            :else (unschedule job-id))
          (let [newjob (try
                         (run job)
                         (catch Throwable e
                           (assoc job
                                  :job/state :job.state/failed
                                  :job/prior-state (:job/state job)
                                  :job/error e
                                  :job/timeout 0)))
                ;; Ensure the job always has the proper id, regardless
                ;; of user error.
                newjob (assoc newjob :job/id job-id)]
            (when (#{:job.state/paused :job.state/failed} (:job/state newjob))
              (async/<! ctrl)) ;; block on a channel event to proceed
            (if (= (:job/state newjob) :job.state/terminated)
              (unschedule job-id) ;; unschedule completely if terminated
              (recur newjob))))))))


;;
;; Listening to the topic bus
;;

(defn- job-message-listener
  [scheduler job-id]
  (reify MessageListener
    (onMessage [_ message]
      (with-scheduler scheduler
        (let [msg (.getMessageObject message)
              job (get-job job-id)
              newjob (try
                       (handle-message job msg)
                       (catch Throwable e
                         (assoc job
                                :job/state :job.state/failed
                                :job/msg msg
                                :job/error-ts (java.util.Date.)
                                :job/error e
                                :job/timeout 0)))
              ;; Ensure the job always has the proper id, regardless
              ;; of user error.
              newjob (assoc newjob :job/id job-id)]
          (put-job! newjob)
          (resume-ctrl job-id))))))

(defn- add-job-listener
  "Add the job topic listener for the current member"
  [scheduler job-id]
  (let [bus-id (.addMessageListener (cc/reliable-topic job-id)
                                    (job-message-listener scheduler job-id))
        job (assoc (get-job job-id) :job/topic-bus bus-id)]
    (put-job! job)))

(defn- remove-job-listener
  "Remove the current listener from a job, e.g. migrating
   job to another member"
  [scheduler job-id]
  (let [job (get-job job-id)]
    (when-let [listener (:job/topic-bus job)]
      (.removeMessageListener (cc/reliable-topic job-id) listener)
      (put-job! (dissoc job :job/topic-bus)))))

;;
;; Migration events
;;

(defn- attach-job
  "Called when job state is being moved to the current node.
   To de-bounce creation, if the job is already on the node,
   don't attach again."
  [scheduler job-id]
  (when-not (job-ctrl job-id)
    (let [job (get-job job-id)]
      (put-job! (assoc job
                       :job/state :job.state/reinit
                       :job/prior-state (:job/state job)
                       :job/timeout 0))
      (run-job scheduler job-id)
      (add-job-listener scheduler job-id))))

(defn- detach-job
  "Called when a job is being moved to another node"
  [scheduler job-id]
  (remove-job-listener scheduler job-id)
  (detach-ctrl job-id))

(defn- local-partition-keys
  "Return the set of keys owned by this cluster
   belonging to the provided partition"
  [^com.hazelcast.core.IMap dmap partid]
  (let [psvc (cc/partition-service)]
    (filter #(== (.getPartitionId (.getPartition psvc %)) partid)
            (keys dmap))))

(defn- ^MigrationListener migration-listener
  [scheduler ctrls]
  (reify
    MigrationListener
    (migrationStarted [_ e]
      (with-scheduler scheduler
        ;; TODO: Record when migration started?
        ))
    (migrationCompleted [_ e]
      (with-scheduler scheduler
        (let [partid (.getPartitionId e)]
          ;; I'm no longer the owner, detach async/loop and msg handler
          (when (.localMember (.getOldOwner e))
            (doseq [job-id (local-partition-keys (cluster-jobs) partid)]
              (detach-job scheduler job-id)))
          ;; I'm the new owner, attach async/loop & msg handler
          (when (.localMember (.getNewOwner e))
            (doseq [job-id (local-partition-keys (cluster-jobs) partid)]
              (attach-job scheduler job-id))))))
    (migrationFailed [_ e]
      (with-scheduler scheduler
        ;; TODO: How to handle failed migrations
        ))))

;;
;; Job Entry Events
;;

(defn- ^MapListener job-entry-listener
  [scheduler ctrls]
  (reify
    EntryAddedListener
    (entryAdded [_ e]
      (with-scheduler scheduler
        (let [job-id (.getKey e)]
          (run-job scheduler job-id)
          (add-job-listener scheduler job-id)
          (record-job-history :create (.getValue e)))))
    EntryUpdatedListener
    (entryUpdated [_ e]
      (with-scheduler scheduler
        (let [job-id (.getKey e)]
          (record-job-history :update (.getValue e)))))
    EntryRemovedListener
    (entryRemoved [_ e]
      (with-scheduler scheduler
        (let [job-id (.getKey e)]
          (remove-job-listener scheduler job-id)
          (remove-ctrl job-id)
          (record-job-history :remove (.getOldValue e)))))))

(defn- resolve-history-fn
  [x]
  (cond
    (symbol? x) (resolve x)
    (var? x) x
    (fn? x) x
    :else (fn [action job-state])))

;;
;; Scheduler Object
;;

(defrecord Scheduler [entry-id migration-id ctrls config node]
  com/Component
  (initialized? [_] true)
  (started? [_] (boolean ctrls))
  (migrate? [_] false)
  (-init [this] this)
  (-start [this]
    (if (thread-bound? #'*scheduler*)
      (set! *scheduler* this)
      (.bindRoot #'*scheduler* this))
    (let [jobs (cluster-jobs (:instance node))
          part (cc/partition-service (:instance node))
          this (assoc this
                      :ctrls (atom {})
                      :config (update config :history-fn resolve-history-fn))
          eid (.addLocalEntryListener jobs (job-entry-listener this ctrls))
          mid (.addMigrationListener part (migration-listener this ctrls))
          local-jobs (seq (.localKeySet jobs))
          this (assoc this
                      :entry-id eid
                      :migration-id mid)]
      (if (thread-bound? #'*scheduler*)
        (set! *scheduler* this)
        (.bindRoot #'*scheduler* this))
      (doseq [job-id local-jobs]
        (attach-job this job-id))
      this))
  (-stop [this]
    ;; For normal cluster shutdown, e.g. during an upgrade, we
    ;; shouldn't remove the node from the map, we should detach the
    ;; ctrl loop and message handler.  If the cluster is being stopped,
    ;; job state will be removed anyway.
    (let [jobs (cluster-jobs (:instance node))]
      (doseq [job-id (seq (.localKeySet jobs))]
        (detach-job this job-id))
      (.removeEntryListener jobs entry-id)
      (.removeMigrationListener (cc/partition-service (:instance node))
                                migration-id)
      (if (thread-bound? #'*scheduler*)
        (set! *scheduler* nil)
        (.bindRoot #'*scheduler* nil))
      (assoc this
             :ctrls nil :entry-id nil :migration-id nil)))
  (-migrate [this] this))



